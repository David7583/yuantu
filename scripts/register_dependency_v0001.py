#!/usr/bin/env python3
# ============================================================
# File: register_dependency_v0001.py
# 中文名: 理解端第三方能力注册脚本
# Version: v0001
# Layer: registry
# Main Layer: understanding
# Updatable: True
#
# Purpose
# 本脚本用于登记与维护“理解端被制度性承认的第三方能力”注册表。
# 注册表为静态制度事实，仅用于表达 alias -> package_name -> version_spec 的映射与用途说明。
#
# What it does
# 1) add: 新增第三方能力制度记录（一个 alias 一个文件）
# 2) get: 查询单个 alias 的制度记录
# 3) list: 列出全部已注册 alias 的摘要
# 4) update: 修订制度记录的可变字段（不允许修改 alias 与注册时间）
# 5) remove: 撤销制度承认（v0001 为删除对应文件）
# 6) dry-run: 对 add/update/remove 输出拟变更结果但不写入文件
#
# What it does NOT do
# 1) 不安装任何第三方库
# 2) 不检查库是否已安装
# 3) 不验证版本是否满足
# 4) 不解析任何业务脚本或源码
# 5) 不记录运行期 used_by 或链路使用情况
# 6) 不影响数据层/行动端/运维端的第三方注册表
#
# Non-v0001 targets (deferred, still recorded here)
# 1) 引入 deprecated 等状态机并采用软删除策略
# 2) 引入更强 schema 校验器并与 validator 家族联动
# 3) 引入跨 layer 的联邦查询与一致性检查
# 4) 引入变更审计日志（jsonl）与回滚工具链
#
# Notes
# - 本脚本是制度注册器，不是运行期解析器
# - 运行期解析与环境检查由 resolve_dependencies_v0001.py 承担
# ============================================================

# ===========================
# ALIAS_META (comment block)
# ===========================
# alias: register_dependency
# family: understanding_dependencies_registry
# role: understanding_third_party_registry
# version: v0001
# status: active
# entry_point: register_dependency_v0001.py
# input:
#   - command add|get|list|update|remove
#   - alias
#   - package_name
#   - version_spec
#   - purpose
#   - replaceable_by
#   - registry_dir (optional)
#   - dry_run (optional)
# output:
#   - json summary to stdout
# depends_on: []
# used_by:
#   - understanding flows
#   - resolve_dependencies_v0001.py (read-only consumer)

# =====================================
# 制度与职责说明注释区
# =====================================
# - 本脚本管理的是理解端第三方能力的“静态制度事实”
# - 本脚本可修改 config/understanding/dependencies/ 下的注册文件
# - 本脚本不进行运行期判断，不做安装，不做环境探测
# - 本脚本不产生“使用事实”，使用事实由理解端登记脚本承担
# - 所有写入操作均支持 dry-run

from __future__ import annotations

# =====================================
# Imports 区
# =====================================
import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# =====================================
# 边界声明与强约束说明
# =====================================
# - 仅使用标准库
# - 不在 import 阶段执行任何写操作
# - 不推断 package 是否存在，不验证版本可用性
# - 写入采用“临时文件 + replace”以减少中断风险
# - dry-run 下不得写入或删除任何文件

# =====================================
# 常量与全局配置区
# =====================================
SCRIPT_NAME = "register_dependency_v0001.py"
SCRIPT_VERSION = "v0001"

DEFAULT_REGISTRY_DIR = Path("config/understanding/dependencies")

UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"
ALIAS_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,63}$")  # 简单、稳定、可审计

STATUS_ACTIVE = "active"

# =====================================
# 工具函数区（无副作用）
# =====================================
def utc_now() -> str:
    return datetime.now(timezone.utc).strftime(UTC_FMT)


def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(dumps_json(data), encoding="utf-8")
    tmp_path.replace(path)


def parse_replaceable_by(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    items = [x.strip() for x in raw.split(",")]
    items = [x for x in items if x]
    return items


def validate_alias(alias: str) -> None:
    if not ALIAS_PATTERN.match(alias):
        raise ValueError(
            "alias 不合法。要求小写字母开头，仅包含小写字母、数字、下划线，长度 2-64。"
        )


def record_path_for_alias(registry_dir: Path, alias: str) -> Path:
    return registry_dir / f"{alias}.json"


def ensure_file_exists(path: Path, alias: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"alias 未注册或文件不存在: {alias} -> {path}")


def ensure_file_not_exists(path: Path, alias: str) -> None:
    if path.exists():
        raise FileExistsError(f"alias 已存在，禁止覆盖: {alias} -> {path}")


def minimal_summary(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "alias": record.get("alias"),
        "package_name": record.get("package_name"),
        "version_spec": record.get("version_spec"),
        "status": record.get("status"),
    }


@dataclass(frozen=True)
class CommandResult:
    status: str
    action: str
    registry_dir: str
    dry_run: bool
    details: Dict[str, Any]


# =====================================
# 核心业务逻辑区
# =====================================
def cmd_add(
    *,
    registry_dir: Path,
    alias: str,
    package_name: str,
    version_spec: str,
    purpose: str,
    replaceable_by: List[str],
    dry_run: bool,
) -> CommandResult:
    validate_alias(alias)
    path = record_path_for_alias(registry_dir, alias)
    ensure_file_not_exists(path, alias)

    record: Dict[str, Any] = {
        "alias": alias,
        "package_name": package_name,
        "version_spec": version_spec,
        "purpose": purpose,
        "replaceable_by": replaceable_by,
        "registered_at": utc_now(),
        "registered_by": SCRIPT_NAME,
        "status": STATUS_ACTIVE,
    }

    if not dry_run:
        atomic_write_json(path, record)

    return CommandResult(
        status="ok",
        action="add",
        registry_dir=str(registry_dir),
        dry_run=dry_run,
        details={
            "record_path": str(path),
            "record": record,
        },
    )


def cmd_get(*, registry_dir: Path, alias: str) -> CommandResult:
    validate_alias(alias)
    path = record_path_for_alias(registry_dir, alias)
    ensure_file_exists(path, alias)
    record = load_json(path)

    return CommandResult(
        status="ok",
        action="get",
        registry_dir=str(registry_dir),
        dry_run=False,
        details={
            "record_path": str(path),
            "record": record,
        },
    )


def cmd_list(*, registry_dir: Path) -> CommandResult:
    registry_dir.mkdir(parents=True, exist_ok=True)
    records: List[Dict[str, Any]] = []

    for p in sorted(registry_dir.glob("*.json")):
        try:
            rec = load_json(p)
            records.append(minimal_summary(rec))
        except Exception as e:
            # list 不应因单个坏文件整体失败，但要显式报告
            records.append({
                "alias": p.stem,
                "package_name": None,
                "version_spec": None,
                "status": "error",
                "error": f"failed_to_read: {type(e).__name__}",
                "path": str(p),
            })

    return CommandResult(
        status="ok",
        action="list",
        registry_dir=str(registry_dir),
        dry_run=False,
        details={
            "count": len(records),
            "items": records,
        },
    )


def cmd_update(
    *,
    registry_dir: Path,
    alias: str,
    package_name: Optional[str],
    version_spec: Optional[str],
    purpose: Optional[str],
    replaceable_by: Optional[List[str]],
    dry_run: bool,
) -> CommandResult:
    validate_alias(alias)
    path = record_path_for_alias(registry_dir, alias)
    ensure_file_exists(path, alias)

    record = load_json(path)

    # 不允许修改 alias / registered_at / registered_by
    before = dict(record)

    if package_name is not None:
        record["package_name"] = package_name
    if version_spec is not None:
        record["version_spec"] = version_spec
    if purpose is not None:
        record["purpose"] = purpose
    if replaceable_by is not None:
        record["replaceable_by"] = replaceable_by

    # v0001 固定 status 为 active，避免引入状态机歧义
    record["status"] = STATUS_ACTIVE

    if record.get("alias") != alias:
        raise ValueError("制度违规：不允许修改 alias")

    if not dry_run:
        atomic_write_json(path, record)

    return CommandResult(
        status="ok",
        action="update",
        registry_dir=str(registry_dir),
        dry_run=dry_run,
        details={
            "record_path": str(path),
            "before": before,
            "after": record,
        },
    )


def cmd_remove(*, registry_dir: Path, alias: str, dry_run: bool) -> CommandResult:
    validate_alias(alias)
    path = record_path_for_alias(registry_dir, alias)
    ensure_file_exists(path, alias)

    record = load_json(path)

    if not dry_run:
        path.unlink()

    return CommandResult(
        status="ok",
        action="remove",
        registry_dir=str(registry_dir),
        dry_run=dry_run,
        details={
            "record_path": str(path),
            "removed_record": record,
        },
    )


# =====================================
# CLI / main 接口区
# =====================================
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Understanding-layer third-party dependency registry (static facts)."
    )
    parser.add_argument(
        "--registry-dir",
        default=str(DEFAULT_REGISTRY_DIR),
        help="Registry directory. Default: config/understanding/dependencies",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_add = sub.add_parser("add", help="Add a new dependency record (one alias one file).")
    p_add.add_argument("--alias", required=True)
    p_add.add_argument("--package", required=True, dest="package_name")
    p_add.add_argument("--version", required=True, dest="version_spec")
    p_add.add_argument("--purpose", required=True)
    p_add.add_argument("--replaceable-by", default="", dest="replaceable_by_raw")
    p_add.add_argument("--dry-run", action="store_true")

    p_get = sub.add_parser("get", help="Get a dependency record by alias.")
    p_get.add_argument("--alias", required=True)

    p_list = sub.add_parser("list", help="List all dependency records (summary).")

    p_update = sub.add_parser("update", help="Update mutable fields of an existing record.")
    p_update.add_argument("--alias", required=True)
    p_update.add_argument("--package", dest="package_name")
    p_update.add_argument("--version", dest="version_spec")
    p_update.add_argument("--purpose")
    p_update.add_argument("--replaceable-by", dest="replaceable_by_raw")
    p_update.add_argument("--dry-run", action="store_true")

    p_remove = sub.add_parser("remove", help="Remove a dependency record by alias.")
    p_remove.add_argument("--alias", required=True)
    p_remove.add_argument("--dry-run", action="store_true")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    registry_dir = Path(args.registry_dir)

    try:
        if args.command == "add":
            res = cmd_add(
                registry_dir=registry_dir,
                alias=args.alias,
                package_name=args.package_name,
                version_spec=args.version_spec,
                purpose=args.purpose,
                replaceable_by=parse_replaceable_by(args.replaceable_by_raw),
                dry_run=bool(args.dry_run),
            )
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "get":
            res = cmd_get(registry_dir=registry_dir, alias=args.alias)
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "list":
            res = cmd_list(registry_dir=registry_dir)
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "update":
            replaceable_by: Optional[List[str]] = None
            if args.replaceable_by_raw is not None:
                # 若用户显式给了参数（即使为空），就覆盖
                replaceable_by = parse_replaceable_by(args.replaceable_by_raw)

            res = cmd_update(
                registry_dir=registry_dir,
                alias=args.alias,
                package_name=args.package_name,
                version_spec=args.version_spec,
                purpose=args.purpose,
                replaceable_by=replaceable_by,
                dry_run=bool(args.dry_run),
            )
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "remove":
            res = cmd_remove(
                registry_dir=registry_dir,
                alias=args.alias,
                dry_run=bool(args.dry_run),
            )
            print(dumps_json(res.__dict__))
            return 0

        print(dumps_json({"status": "error", "error": "unknown_command"}))
        return 2

    except Exception as e:
        err = {
            "status": "error",
            "error_type": type(e).__name__,
            "error": str(e),
            "command": getattr(args, "command", None),
            "registry_dir": str(registry_dir),
        }
        print(dumps_json(err))
        return 1


# =====================================
# Entry Point
# =====================================
if __name__ == "__main__":
    raise SystemExit(main())
