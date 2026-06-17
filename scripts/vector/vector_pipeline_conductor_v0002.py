#!/usr/bin/env python3
# ============================================================
# File: vector_pipeline_conductor_v0002.py
# 中文名: 向量流水线调度脚本
# Version: v0002
# Layer: orchestration
# Main Layer: understand
# Updatable: True
#
# Purpose:
# 调度向量化流水线的四个阶段（生成→校验→生命周期→写入），
# 以"移动产物、保留留档"的方式控制磁盘占用。
#
# What it does:
# 1. 按批次（对话数量切片）循环调度四阶段子脚本
# 2. 解析每阶段子进程的结构化 JSON 输出，判断成功/失败
# 3. 阶段间产物采用 shutil.move（非 copy），仅保留各阶段 run_meta.json
# 4. 每批次结束后清理最终阶段的 JSONL 产物（writer 写入 ChromaDB 后不再需要）
# 5. 写入调度层 run_meta.json 汇总审计（含各批次结果、耗时、错误链）
# 6. 支持 --dry-run 预览全流程，不执行实际操作
# 7. 支持 --target 指定向量化目标类型（instance/concept）
#
# What it does NOT do:
# 1. 不直接操作 SQL 或 ChromaDB（由子脚本负责）
# 2. 不做契约校验或生命周期判定（由对应子脚本负责）
# 3. 不生成 embedding 向量（由 generator 负责）
#
# 存储策略:
# - 阶段间传递: move（节省磁盘）
# - 各阶段 run_meta.json: 保留（审计需要）
# - 最终 JSONL 产物: 写入 ChromaDB 后删除（writer 已完成使命）
# - 调度层 run_meta.json: 始终写入（汇总级审计）
#
# 制度职责声明:
# - 是否进行制度判断: 否（纯调度，不做业务裁决）
# - 是否修改系统对象: 间接（通过子脚本修改 ChromaDB 和状态索引）
# ============================================================

# ============================================================
# ALIAS_META
# alias: vector_pipeline_conductor
# family: vector_orchestration
# role: conductor
# version: v0002
# status: active
# entry_point: scripts/vector/vector_pipeline_conductor_v0002.py
# input: CLI 参数（批次大小、目标类型、轮数等）
# output: conductor_run_meta.json 汇总审计
# depends_on: embedding_generator, embedding_contract_validator, embedding_lifecycle_guard, embedding_writer
# used_by: manual trigger, cron
# ============================================================

# ============================================================
# 制度与职责说明注释区
#
# - 是否进行合规判断: 否
# - 是否进行制度判断: 否
# - 是否修改系统对象: 间接（调度子脚本执行修改）
# - 是否解析其他头部结构: 否
# ============================================================

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ============================================================
# 常量与全局配置区
# ============================================================

SCRIPT_NAME = "vector_pipeline_conductor"
SCRIPT_VERSION = "v0002"
MAIN_LAYER = "understand"

MAX_ROOT_SEARCH_DEPTH = 10

# 流水线阶段目录名
STAGE_1_GENERATED = "1_generated"
STAGE_2_VALIDATED = "2_validated"
STAGE_3_LIFECYCLE = "3_lifecycle_checked"

# 子脚本相对路径（相对于 project_root）
SCRIPT_GENERATOR = Path("scripts") / "vector" / "embedding_generator_v0001.py"
SCRIPT_VALIDATOR = Path("scripts") / "vector" / "embedding_contract_validator_v0001.py"
SCRIPT_LIFECYCLE = Path("scripts") / "vector" / "embedding_lifecycle_guard_v0001.py"
SCRIPT_WRITER = Path("scripts") / "vector" / "embedding_writer_v0001.py"

# 默认值
DEFAULT_BATCH_SIZE = 10
DEFAULT_MAX_BATCHES = 2
DEFAULT_TARGET = "instance"
DEFAULT_PIPELINE_BASE = Path("vector") / "pipeline"

# ============================================================
# 工具函数区（无副作用）
# ============================================================


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _timestamp_for_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _pretty_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _find_project_root(start: Path, max_up: int = MAX_ROOT_SEARCH_DEPTH) -> Path:
    cur = start.resolve()
    for _ in range(max_up):
        if (cur / "scripts").is_dir() and (cur / "config").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return Path.cwd().resolve()


def _print_stage(msg: str) -> None:
    """调度层日志（打印到 stderr，不污染 stdout 的结构化输出）"""
    print(f"[{SCRIPT_NAME}] {msg}", file=sys.stderr)


# ============================================================
# 子进程调用
# ============================================================


def _run_child(
    cmd: List[str],
    cwd: Path,
    stage_name: str,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    执行子脚本并解析其结构化 JSON 输出。

    返回:
    {
        "success": bool,
        "stage": str,
        "exit_code": int,
        "stdout_json": dict or None,
        "stderr_tail": str,
        "elapsed_seconds": float,
        "command": str,
    }
    """
    cmd_str = " ".join(str(c) for c in cmd)
    _print_stage(f"[{stage_name}] 执行: {cmd_str}")

    if dry_run:
        _print_stage(f"[{stage_name}] [DRY RUN] 跳过实际执行")
        return {
            "success": True,
            "stage": stage_name,
            "exit_code": 0,
            "stdout_json": {"status": "dry-run", "action": stage_name},
            "stderr_tail": "",
            "elapsed_seconds": 0.0,
            "command": cmd_str,
        }

    t0 = time.monotonic()

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=str(cwd),
    )

    elapsed = round(time.monotonic() - t0, 2)

    # 解析 stdout JSON（子脚本规范：退出前打印一次结构化 JSON）
    stdout_json = None
    stdout_text = (result.stdout or "").strip()
    if stdout_text:
        try:
            stdout_json = json.loads(stdout_text)
        except json.JSONDecodeError:
            # stdout 可能包含非 JSON 内容（如进度条），取最后一段尝试
            for line in reversed(stdout_text.splitlines()):
                line = line.strip()
                if line.startswith("{"):
                    try:
                        stdout_json = json.loads(line)
                        break
                    except json.JSONDecodeError:
                        continue

    # stderr 尾部（用于调试，限制长度）
    stderr_text = (result.stderr or "").strip()
    stderr_tail = stderr_text[-2000:] if len(stderr_text) > 2000 else stderr_text

    success = result.returncode == 0

    if not success:
        _print_stage(f"[{stage_name}] 失败 (exit_code={result.returncode})")
        if stderr_tail:
            _print_stage(f"[{stage_name}] stderr: {stderr_tail[:500]}")

    return {
        "success": success,
        "stage": stage_name,
        "exit_code": result.returncode,
        "stdout_json": stdout_json,
        "stderr_tail": stderr_tail,
        "elapsed_seconds": elapsed,
        "command": cmd_str,
    }


# ============================================================
# 阶段间产物移动（核心存储策略）
# ============================================================


def _move_jsonl_preserve_meta(
    source_dir: Path,
    target_dir: Path,
    stage_name: str,
) -> Dict[str, Any]:
    """
    将 source_dir 的 JSONL 产物移动到 target_dir，
    但保留 source_dir 中的 run_meta.json（审计需要）。

    策略:
    - .jsonl 文件: shutil.move（节省磁盘）
    - run_meta.json: 保留在原位
    - 其他 .json 文件（如 lifecycle_actions 等辅助文件）: move
    - 子目录（如 instance/, concept/）: 递归处理

    返回操作摘要。
    """
    if not source_dir.exists():
        return {"status": "skipped", "reason": f"source not found: {source_dir}"}

    _ensure_dir(target_dir)

    moved_files = 0
    preserved_meta = False

    # 处理顶层文件
    for f in source_dir.iterdir():
        if f.is_file():
            if f.name == "run_meta.json":
                # 保留 run_meta.json 在原位
                preserved_meta = True
                continue
            # 移动所有其他文件
            shutil.move(str(f), str(target_dir / f.name))
            moved_files += 1

    # 处理子目录（instance/, concept/ 等）
    for subdir in source_dir.iterdir():
        if subdir.is_dir():
            target_subdir = target_dir / subdir.name
            _ensure_dir(target_subdir)
            for f in subdir.iterdir():
                if f.is_file():
                    shutil.move(str(f), str(target_subdir / f.name))
                    moved_files += 1
            # 如果子目录已空，清理
            if not any(subdir.iterdir()):
                subdir.rmdir()

    return {
        "status": "ok",
        "stage": stage_name,
        "source": str(source_dir),
        "target": str(target_dir),
        "moved_files": moved_files,
        "preserved_meta": preserved_meta,
    }


def _cleanup_final_jsonl(directory: Path, stage_name: str) -> Dict[str, Any]:
    """
    清理最终阶段的 JSONL 产物（writer 写入 ChromaDB 后不再需要）。
    保留 run_meta.json 和其他审计文件。
    """
    if not directory.exists():
        return {"status": "skipped", "reason": f"directory not found: {directory}"}

    removed = 0

    # 递归删除 .jsonl 文件
    for jsonl_file in directory.rglob("*.jsonl"):
        jsonl_file.unlink()
        removed += 1

    # 清理空子目录
    for subdir in sorted(directory.rglob("*"), reverse=True):
        if subdir.is_dir() and not any(subdir.iterdir()):
            subdir.rmdir()

    return {
        "status": "ok",
        "stage": stage_name,
        "directory": str(directory),
        "jsonl_removed": removed,
    }


# ============================================================
# 数据耗尽检测
# ============================================================


def _check_data_exhausted(stdout_json: Optional[Dict[str, Any]]) -> bool:
    """
    检查 generator 输出是否表明数据已耗尽。

    generator 在无数据时会打印:
    {"status": "info", "message": "No matching records found in specified range."}
    """
    if stdout_json is None:
        return False

    # 检查直接的 info 消息
    if stdout_json.get("status") == "info":
        msg = stdout_json.get("message", "")
        if "No matching records found" in msg:
            return True

    # 检查 total_generated == 0 且 status == success
    if (
        stdout_json.get("status") == "success"
        and stdout_json.get("total_generated", -1) == 0
    ):
        return True

    return False


# ============================================================
# 单批次流水线
# ============================================================


def _run_single_batch(
    project_root: Path,
    pipeline_base: Path,
    batch_id: str,
    target: str,
    batch_size: int,
    offset: int,
    mock_api: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    """
    执行单个批次的四阶段流水线。

    返回批次结果摘要。
    """
    batch_result: Dict[str, Any] = {
        "batch_id": batch_id,
        "offset": offset,
        "batch_size": batch_size,
        "status": "started",
        "stages": {},
        "moves": {},
        "cleanup": {},
        "started_at": _utc_iso(),
        "error": None,
        "data_exhausted": False,
    }

    # 计算各阶段目录（绝对路径）
    abs_pipeline = project_root / pipeline_base
    dir_1 = abs_pipeline / STAGE_1_GENERATED / batch_id
    dir_2 = abs_pipeline / STAGE_2_VALIDATED / batch_id
    dir_3 = abs_pipeline / STAGE_3_LIFECYCLE / batch_id

    # ── 阶段 1: Generator ──────────────────────────────
    gen_cmd = [
        sys.executable,
        str(project_root / SCRIPT_GENERATOR),
        "--run-id", batch_id,
        "--target", target,
        "--all-assets",
        "--limit-dialogs", str(batch_size),
        "--offset-dialogs", str(offset),
    ]
    if mock_api:
        gen_cmd.append("--mock-api")

    gen_result = _run_child(gen_cmd, project_root, "generator", dry_run=dry_run)
    batch_result["stages"]["generator"] = gen_result

    # 检查数据耗尽
    if _check_data_exhausted(gen_result.get("stdout_json")):
        _print_stage(f"[batch:{batch_id}] 数据已耗尽，停止后续批次。")
        batch_result["status"] = "data_exhausted"
        batch_result["data_exhausted"] = True
        batch_result["finished_at"] = _utc_iso()
        return batch_result

    if not gen_result["success"]:
        batch_result["status"] = "failed"
        batch_result["error"] = f"generator failed (exit_code={gen_result['exit_code']})"
        batch_result["finished_at"] = _utc_iso()
        return batch_result

    # ── 阶段 2: Validator ──────────────────────────────
    # validator 使用 --input-dir 读取，--promote 不启用（由 conductor 自行 move）
    val_cmd = [
        sys.executable,
        str(project_root / SCRIPT_VALIDATOR),
        "--input-dir", str(dir_1),
    ]

    val_result = _run_child(val_cmd, project_root, "validator", dry_run=dry_run)
    batch_result["stages"]["validator"] = val_result

    if not val_result["success"]:
        batch_result["status"] = "failed"
        batch_result["error"] = f"validator failed (exit_code={val_result['exit_code']})"
        batch_result["finished_at"] = _utc_iso()
        return batch_result

    # 校验通过后，移动产物到 2_validated（保留 1_generated 的 run_meta）
    if not dry_run:
        move_1to2 = _move_jsonl_preserve_meta(dir_1, dir_2, "1→2")
        batch_result["moves"]["1_to_2"] = move_1to2
    else:
        batch_result["moves"]["1_to_2"] = {"status": "dry-run"}

    # ── 阶段 3: Lifecycle Guard ────────────────────────
    life_cmd = [
        sys.executable,
        str(project_root / SCRIPT_LIFECYCLE),
        "--input-dir", str(dir_2),
        "--output-dir", str(dir_3),
    ]

    life_result = _run_child(life_cmd, project_root, "lifecycle", dry_run=dry_run)
    batch_result["stages"]["lifecycle"] = life_result

    if not life_result["success"]:
        batch_result["status"] = "failed"
        batch_result["error"] = f"lifecycle failed (exit_code={life_result['exit_code']})"
        batch_result["finished_at"] = _utc_iso()
        return batch_result

    # lifecycle 完成后，清理 2_validated 的 JSONL（保留 run_meta）
    if not dry_run:
        cleanup_2 = _cleanup_final_jsonl(dir_2, "cleanup_2_validated")
        batch_result["cleanup"]["2_validated"] = cleanup_2
    else:
        batch_result["cleanup"]["2_validated"] = {"status": "dry-run"}

    # ── 阶段 4: Writer ─────────────────────────────────
    write_cmd = [
        sys.executable,
        str(project_root / SCRIPT_WRITER),
        "--input-dir", str(dir_3),
    ]

    write_result = _run_child(write_cmd, project_root, "writer", dry_run=dry_run)
    batch_result["stages"]["writer"] = write_result

    if not write_result["success"]:
        batch_result["status"] = "failed"
        batch_result["error"] = f"writer failed (exit_code={write_result['exit_code']})"
        batch_result["finished_at"] = _utc_iso()
        return batch_result

    # writer 完成后，清理 3_lifecycle_checked 的 JSONL（保留 run_meta）
    if not dry_run:
        cleanup_3 = _cleanup_final_jsonl(dir_3, "cleanup_3_lifecycle")
        batch_result["cleanup"]["3_lifecycle"] = cleanup_3
    else:
        batch_result["cleanup"]["3_lifecycle"] = {"status": "dry-run"}

    batch_result["status"] = "success"
    batch_result["finished_at"] = _utc_iso()
    return batch_result


# ============================================================
# 调度主逻辑
# ============================================================


def conduct_pipeline(
    project_root: Path,
    pipeline_base: Path,
    target: str,
    batch_size: int,
    max_batches: int,
    mock_api: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    """
    调度主函数：按批次循环执行四阶段流水线。
    """
    conductor_started = _utc_iso()
    conductor_run_id = f"conductor_{_timestamp_for_id()}"

    _print_stage(f"run_id: {conductor_run_id}")
    _print_stage(f"target: {target}, batch_size: {batch_size}, max_batches: {max_batches}")
    _print_stage(f"project_root: {project_root}")
    if dry_run:
        _print_stage("*** DRY RUN 模式 ***")

    batch_results: List[Dict[str, Any]] = []
    total_batches_run = 0
    total_batches_success = 0
    stop_reason: Optional[str] = None

    for i in range(max_batches):
        current_offset = i * batch_size
        batch_id = f"batch_{_timestamp_for_id()}_{i:03d}"

        _print_stage(f"\n{'='*60}")
        _print_stage(f"批次 {i+1}/{max_batches}: {batch_id} (offset={current_offset})")
        _print_stage(f"{'='*60}")

        batch_result = _run_single_batch(
            project_root=project_root,
            pipeline_base=pipeline_base,
            batch_id=batch_id,
            target=target,
            batch_size=batch_size,
            offset=current_offset,
            mock_api=mock_api,
            dry_run=dry_run,
        )

        batch_results.append(batch_result)
        total_batches_run += 1

        if batch_result["status"] == "success":
            total_batches_success += 1
            _print_stage(f"批次 {batch_id} 完成。")

        elif batch_result["status"] == "data_exhausted":
            stop_reason = "data_exhausted"
            _print_stage("所有数据已处理完毕，停止调度。")
            break

        elif batch_result["status"] == "failed":
            stop_reason = f"batch_failed: {batch_result.get('error', 'unknown')}"
            _print_stage(f"批次 {batch_id} 失败，停止调度。")
            break

    else:
        # 正常耗尽 max_batches
        stop_reason = "max_batches_reached"

    conductor_finished = _utc_iso()

    # 构建调度层汇总
    overall_status = "success"
    if any(b["status"] == "failed" for b in batch_results):
        overall_status = "partial_failure" if total_batches_success > 0 else "failed"

    conductor_meta = {
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "main_layer": MAIN_LAYER,
        "conductor_run_id": conductor_run_id,
        "target": target,
        "batch_size": batch_size,
        "max_batches": max_batches,
        "mock_api": mock_api,
        "dry_run": dry_run,
        "status": overall_status,
        "stop_reason": stop_reason,
        "total_batches_run": total_batches_run,
        "total_batches_success": total_batches_success,
        "started_at": conductor_started,
        "finished_at": conductor_finished,
        "batches": batch_results,
    }

    # 写入调度层 run_meta.json
    if not dry_run:
        try:
            log_dir = project_root / pipeline_base / "conductor_logs"
            _ensure_dir(log_dir)
            meta_file = log_dir / f"{conductor_run_id}.json"
            meta_file.write_text(_pretty_json(conductor_meta), encoding="utf-8")
            _print_stage(f"调度留档: {meta_file}")
        except Exception as e:
            _print_stage(f"[警告] 调度留档写入失败: {e}")
            conductor_meta["log_write_error"] = str(e)

    return conductor_meta


# ============================================================
# CLI / main 接口区
# ============================================================


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "向量化流水线调度脚本：按批次循环调度 generator → validator → lifecycle → writer，"
            "以移动产物、保留留档的方式控制磁盘占用。"
        ),
    )
    ap.add_argument(
        "--target",
        default=DEFAULT_TARGET,
        choices=["instance", "concept"],
        help=f"向量化目标类型。默认: {DEFAULT_TARGET}",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"每批次处理的对话数量。默认: {DEFAULT_BATCH_SIZE}",
    )
    ap.add_argument(
        "--max-batches",
        type=int,
        default=DEFAULT_MAX_BATCHES,
        help=f"本次运行最大批次数。默认: {DEFAULT_MAX_BATCHES}",
    )
    ap.add_argument(
        "--mock-api",
        action="store_true",
        help="使用 mock 随机向量（测试用，不调用真实 API）。",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="预览模式：打印命令但不执行子进程，不移动/删除文件。",
    )
    ap.add_argument(
        "--pipeline-base",
        default=str(DEFAULT_PIPELINE_BASE),
        help=f"流水线基础目录（相对于项目根）。默认: {DEFAULT_PIPELINE_BASE}",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    here = Path(__file__).resolve()
    project_root = _find_project_root(here)

    result = conduct_pipeline(
        project_root=project_root,
        pipeline_base=Path(args.pipeline_base),
        target=args.target,
        batch_size=args.batch_size,
        max_batches=args.max_batches,
        mock_api=args.mock_api,
        dry_run=args.dry_run,
    )

    # 最终结构化输出（stdout），供上层脚本或人工检查
    print(_pretty_json(result))

    if result.get("status") == "failed":
        return 1
    return 0


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())
