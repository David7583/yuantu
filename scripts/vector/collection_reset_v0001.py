#!/usr/bin/env python3
# ============================================================
# File: collection_reset_v0001.py
# 中文名: 向量库集合重置脚本
# Version: v0001
# Layer: infrastructure
# Main Layer: understand
# Updatable: True
#
# Purpose:
# 以可审计、可追溯的方式删除 ChromaDB collection，
# 为重建（如距离度量迁移）提供有记录的操作入口。
#
# What it does:
# 1. 确认目标 collection 是否存在
# 2. 记录删除前的 collection 快照（条数、metadata 等）
# 3. 执行删除操作
# 4. 写入 run_meta.json 留档（时间、原因、操作结果）
# 5. 支持 --dry-run 预览，不执行实际删除
#
# What it does NOT do:
# 1. 不删除 ChromaDB 目录本身（只删 collection）
# 2. 不删除 SQL 数据
# 3. 不删除向量 pipeline 中间文件
# 4. 不重建 collection（由 embedding_writer 负责）
#
# 典型使用场景:
# - 距离度量迁移（L2 → cosine）
# - collection 损坏需要重建
# - 测试环境清理
#
# 制度职责声明:
# - 是否进行制度判断: 否
# - 是否修改系统对象: 是（删除 ChromaDB collection）
# ============================================================

# ============================================================
# ALIAS_META
# alias: collection_reset
# family: vector_infrastructure
# role: infrastructure_reset
# version: v0001
# status: active
# entry_point: scripts/vector/collection_reset_v0001.py
# input: chromadb_path, collection_name, reason
# output: run_meta.json 留档
# depends_on: chromadb, Python stdlib
# used_by: manual trigger, migration flow
# ============================================================

# ============================================================
# 制度与职责说明注释区
#
# - 是否进行合规判断: 否
# - 是否进行制度判断: 否
# - 是否修改系统对象: 是（破坏性操作，需 --confirm 显式确认）
# - 是否解析其他头部结构: 否
# ============================================================

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# ============================================================
# 常量与全局配置区
# ============================================================

SCRIPT_NAME = "collection_reset"
SCRIPT_VERSION = "v0001"
MAIN_LAYER = "understand"

# 项目根目录锚定（从脚本位置向上两级：scripts/vector/ → scripts/ → project_root/）
_THIS_FILE = Path(__file__).resolve()
_PROJECT_ROOT = _THIS_FILE.parent.parent.parent

# 默认值（基于项目根目录的绝对路径，防止从任意目录运行时路径错误）
DEFAULT_CHROMADB_PATH = _PROJECT_ROOT / "chromadb/understand/vectors"
DEFAULT_COLLECTION_NAME = "understand_embeddings"
DEFAULT_LOG_DIR = _PROJECT_ROOT / "chromadb/understand/reset_logs"

# ============================================================
# 工具函数区（无副作用）
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _pretty_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _print_stderr(msg: str) -> None:
    print(msg, file=sys.stderr)


def _timestamp_for_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


# ============================================================
# ChromaDB 操作
# ============================================================

def _connect_chromadb(chromadb_path: Path):
    """连接 ChromaDB，返回 client"""
    try:
        import chromadb
        from chromadb.config import Settings
    except ImportError:
        raise RuntimeError("chromadb is required. Run: pip install chromadb")

    if not chromadb_path.exists():
        raise FileNotFoundError(f"ChromaDB path not found: {chromadb_path}")

    client = chromadb.PersistentClient(
        path=str(chromadb_path),
        settings=Settings(anonymized_telemetry=False),
    )
    return client


def _get_collection_snapshot(client, collection_name: str) -> Dict[str, Any]:
    """
    获取 collection 的快照信息（删除前留档用）。

    返回:
    {
        "found": True/False,
        "name": ...,
        "count": ...,
        "metadata": ...,
        "error": None 或错误信息字符串（非"不存在"类错误）
    }
    """
    try:
        collection = client.get_collection(name=collection_name)
        count = collection.count()
        metadata = collection.metadata or {}
        return {
            "found": True,
            "name": collection_name,
            "count": count,
            "metadata": metadata,
            "error": None,
        }
    except ValueError:
        # ChromaDB 抛出 ValueError 表示 collection 不存在
        return {
            "found": False,
            "name": collection_name,
            "count": None,
            "metadata": None,
            "error": None,
        }
    except Exception as e:
        # 其他异常（磁盘错误、权限不足、数据库损坏等）需要暴露
        return {
            "found": False,
            "name": collection_name,
            "count": None,
            "metadata": None,
            "error": f"Unexpected error while reading collection: {e}",
        }


# ============================================================
# 核心业务逻辑
# ============================================================

def process_reset(
    chromadb_path: Path,
    collection_name: str,
    reason: str,
    log_dir: Path,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    执行 collection 重置的主逻辑。

    Args:
        chromadb_path: ChromaDB 持久化路径
        collection_name: 要删除的 collection 名称
        reason: 删除原因（必填，写入留档）
        log_dir: 留档目录
        dry_run: 是否仅预览，不执行实际删除

    Returns:
        结构化操作结果
    """
    started_at = _utc_iso()
    run_id = f"reset_{_timestamp_for_id()}"
    error_msg: Optional[str] = None
    collection_snapshot: Dict[str, Any] = {}
    status = "success"

    _print_stderr(f"[{SCRIPT_NAME}] run_id: {run_id}")
    _print_stderr(f"[{SCRIPT_NAME}] target: {collection_name} @ {chromadb_path}")
    _print_stderr(f"[{SCRIPT_NAME}] reason: {reason}")
    if dry_run:
        _print_stderr(f"[{SCRIPT_NAME}] *** DRY RUN 模式，不执行实际删除 ***")

    # 连接 ChromaDB
    client = None
    try:
        client = _connect_chromadb(chromadb_path)
    except Exception as e:
        error_msg = f"Failed to connect to ChromaDB: {e}"

    # 获取删除前快照
    if error_msg is None and client is not None:
        collection_snapshot = _get_collection_snapshot(client, collection_name)

        if collection_snapshot.get("error"):
            # 非"不存在"类错误（磁盘损坏、权限等），阻断操作
            error_msg = collection_snapshot["error"]
        elif not collection_snapshot.get("found"):
            error_msg = f"Collection '{collection_name}' not found. Nothing to delete."
        else:
            _print_stderr(
                f"[{SCRIPT_NAME}] collection snapshot: "
                f"count={collection_snapshot['count']}, "
                f"metadata={collection_snapshot['metadata']}"
            )

    # 执行删除
    deleted = False
    if error_msg is None and client is not None:
        if dry_run:
            _print_stderr(f"[{SCRIPT_NAME}] [DRY RUN] 将删除 collection: {collection_name}")
            deleted = False
        else:
            try:
                client.delete_collection(name=collection_name)
                deleted = True
                _print_stderr(f"[{SCRIPT_NAME}] collection '{collection_name}' 已删除。")
            except Exception as e:
                error_msg = f"Failed to delete collection: {e}"

    if error_msg is not None:
        status = "failed"

    finished_at = _utc_iso()

    # 构建 run_meta
    meta = {
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "main_layer": MAIN_LAYER,
        "run_id": run_id,
        "chromadb_path": str(chromadb_path),
        "collection_name": collection_name,
        "reason": reason,
        "dry_run": dry_run,
        "status": status,
        "deleted": deleted,
        "collection_snapshot_before": collection_snapshot,
        "error": error_msg,
        "started_at": started_at,
        "finished_at": finished_at,
    }

    # 写入留档
    if not dry_run:
        try:
            _ensure_dir(log_dir)
            meta_file = log_dir / f"{run_id}.json"
            meta_file.write_text(_pretty_json(meta), encoding="utf-8")
            _print_stderr(f"[{SCRIPT_NAME}] 留档: {meta_file}")
        except Exception as e:
            # 留档是审计核心需求，失败不能静默通过
            _print_stderr(f"[{SCRIPT_NAME}] [错误] 留档写入失败: {e}")
            meta["status"] = "partial_success"
            meta["log_error"] = str(e)

    return meta


# ============================================================
# CLI / main 接口区
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="以可审计方式删除 ChromaDB collection，为重建操作留档。"
    )
    ap.add_argument(
        "--chromadb-path",
        default=str(DEFAULT_CHROMADB_PATH),
        help=f"ChromaDB 持久化路径。默认: {DEFAULT_CHROMADB_PATH}",
    )
    ap.add_argument(
        "--collection-name",
        default=DEFAULT_COLLECTION_NAME,
        help=f"要删除的 collection 名称。默认: {DEFAULT_COLLECTION_NAME}",
    )
    ap.add_argument(
        "--reason",
        required=True,
        help="删除原因（必填，写入留档）。例如: 'L2迁移至cosine距离度量'",
    )
    ap.add_argument(
        "--log-dir",
        default=str(DEFAULT_LOG_DIR),
        help=f"留档目录。默认: {DEFAULT_LOG_DIR}",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="预览模式，不执行实际删除。",
    )
    ap.add_argument(
        "--confirm",
        action="store_true",
        help="确认执行删除（非 dry-run 模式下必须传入此参数）。",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    # 安全锁：非 dry-run 必须显式传 --confirm
    if not args.dry_run and not args.confirm:
        _print_stderr(
            "[错误] 这是破坏性操作。请传入 --confirm 参数确认执行，"
            "或使用 --dry-run 预览。"
        )
        return 1

    chromadb_path = Path(args.chromadb_path)
    collection_name = args.collection_name
    log_dir = Path(args.log_dir)

    result = process_reset(
        chromadb_path=chromadb_path,
        collection_name=collection_name,
        reason=args.reason,
        log_dir=log_dir,
        dry_run=args.dry_run,
    )

    print(_pretty_json(result))

    if result.get("status") == "failed":
        return 1
    return 0


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())
