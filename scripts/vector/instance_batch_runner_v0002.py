#!/usr/bin/env python3
# ============================================================
# File: instance_batch_runner_v0002.py
# 中文名: Instance 向量化自动调度脚本
# Version: v0002
# Layer: orchestration
# Main Layer: understand
# Updatable: True
#
# Purpose:
# 傻瓜式 instance 向量化：开了就跑，关了就停，下次开了接着跑。
# 所有运行参数从 config YAML 读取，日常使用无需传任何参数。
#
# What it does:
# 1. 读取 instance_runner_config_v0002.yml 获取阈值与批次参数
# 2. 自动检测上次进度（扫描留档）
# 3. 持续跑批次，每批次完成后：
#    a. 写入留档（断点安全）
#    b. 打印进度条
#    c. 检查是否达到阈值 → 达到则停止
#    d. 检查信号文件 STOP → 存在则优雅停止
# 4. 停止点永远在批次之间（无脏数据，无需残留清理）
# 5. 复用四阶段流水线：generator → validator → lifecycle → writer
# 6. 移动产物、保留 run_meta.json（节省磁盘）
#
# What it does NOT do:
# 1. 不需要手动传 offset 或任何断点参数
# 2. 不处理 concept（concept 用 conductor_v0002）
# 3. 不直接操作 SQL 或 ChromaDB（由子脚本负责）
# 4. 不在批次中间停止（因此不需要残留清理逻辑）
#
# 使用方式:
#   # 永远就这一个命令
#   python scripts\vector\instance_batch_runner_v0002.py
#
#   # 想提前停止（另开终端）:
#   echo stop > vector\pipeline\STOP
#
#   # 想调整单次运行量:
#   修改 config\instance_runner_config_v0002.yml 中的 max_dialogs_per_run
#
# 制度职责声明:
# - 是否进行制度判断: 否（纯调度）
# - 是否修改系统对象: 间接（通过子脚本）
# ============================================================

# ============================================================
# ALIAS_META
# alias: instance_batch_runner
# family: vector_orchestration
# role: instance_runner
# version: v0002
# status: active
# entry_point: scripts/vector/instance_batch_runner_v0002.py
# input: instance_runner_config_v0002.yml, 历史留档
# output: instance_runner_logs 留档
# depends_on: embedding_generator, embedding_contract_validator, embedding_lifecycle_guard, embedding_writer
# used_by: manual trigger
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
# 常量
# ============================================================

SCRIPT_NAME = "instance_batch_runner"
SCRIPT_VERSION = "v0002"
MAIN_LAYER = "understand"
MAX_ROOT_SEARCH_DEPTH = 10

# 子脚本相对路径
SCRIPT_GENERATOR = Path("scripts") / "vector" / "embedding_generator_v0003.py"
SCRIPT_VALIDATOR = Path("scripts") / "vector" / "embedding_contract_validator_v0001.py"
SCRIPT_LIFECYCLE = Path("scripts") / "vector" / "embedding_lifecycle_guard_v0001.py"
SCRIPT_WRITER = Path("scripts") / "vector" / "embedding_writer_v0001.py"

# 流水线阶段
STAGE_1_GENERATED = "1_generated"
STAGE_2_VALIDATED = "2_validated"
STAGE_3_LIFECYCLE = "3_lifecycle_checked"

# 固定路径
PIPELINE_BASE = Path("vector") / "pipeline"
LOG_DIR_NAME = "instance_runner_logs"
DEFAULT_CONFIG_PATH = Path("config") / "instance_runner_config_v0002.yml"

# ============================================================
# 工具函数
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


def _log(msg: str) -> None:
    print(f"[instance_runner] {msg}", file=sys.stderr)


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}秒"
    elif seconds < 3600:
        return f"{seconds/60:.0f}分钟"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}小时{m}分钟"


def _format_size(byte_count: float) -> str:
    if byte_count < 1024 ** 3:
        return f"{byte_count / 1024**2:.1f} GB"
    else:
        return f"{byte_count / 1024**3:.2f} TB"


# ============================================================
# Config 读取
# ============================================================


def _load_config(project_root: Path) -> Dict[str, Any]:
    """
    读取 config YAML，返回扁平化参数字典。
    如果 config 不存在或读取失败，使用安全默认值。
    """
    defaults = {
        "dialogs_per_batch": 20,
        "max_dialogs_per_run": 400,
        "total_dialogs": 2170,
        "est_instances_per_dialog": 4000,
        "est_seconds_per_instance": 0.020,
        "est_bytes_per_instance": 6000,
        "stop_file": "vector/pipeline/STOP",
    }

    config_path = project_root / DEFAULT_CONFIG_PATH

    if not config_path.exists():
        _log(f"[警告] config 未找到: {config_path}，使用默认值。")
        return defaults

    try:
        import yaml
    except ImportError:
        _log("[警告] PyYAML 未安装，使用默认值。")
        return defaults

    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            _log(f"[警告] config 格式异常，使用默认值。")
            return defaults

        batch_cfg = raw.get("batch", {}) or {}
        run_cfg = raw.get("run", {}) or {}
        data_cfg = raw.get("data", {}) or {}
        est_cfg = raw.get("estimate", {}) or {}
        signal_cfg = raw.get("signal", {}) or {}

        return {
            "dialogs_per_batch": int(batch_cfg.get("dialogs_per_batch", defaults["dialogs_per_batch"])),
            "max_dialogs_per_run": int(run_cfg.get("max_dialogs_per_run", defaults["max_dialogs_per_run"])),
            "total_dialogs": int(data_cfg.get("total_dialogs", defaults["total_dialogs"])),
            "est_instances_per_dialog": int(est_cfg.get("instances_per_dialog", defaults["est_instances_per_dialog"])),
            "est_seconds_per_instance": float(est_cfg.get("seconds_per_instance", defaults["est_seconds_per_instance"])),
            "est_bytes_per_instance": int(est_cfg.get("bytes_per_instance", defaults["est_bytes_per_instance"])),
            "stop_file": str(signal_cfg.get("stop_file", defaults["stop_file"])),
        }
    except Exception as e:
        _log(f"[警告] config 读取失败: {e}，使用默认值。")
        return defaults


# ============================================================
# 进度检测
# ============================================================


def _detect_progress(project_root: Path) -> int:
    """扫描留档目录，推算上次成功跑到的 offset。"""
    max_offset = 0

    log_dir = project_root / PIPELINE_BASE / LOG_DIR_NAME
    if not log_dir.exists():
        return 0

    for log_file in log_dir.glob("*.json"):
        try:
            data = json.loads(log_file.read_text(encoding="utf-8"))
            if data.get("target") != "instance":
                continue
            start = data.get("start_offset", 0)
            success = data.get("total_batches_success", 0)
            bsize = data.get("batch_size", 0)
            end_offset = start + success * bsize
            if end_offset > max_offset:
                max_offset = end_offset
        except Exception:
            continue

    return max_offset


# ============================================================
# 信号文件检测
# ============================================================


def _check_stop_signal(project_root: Path, stop_file_rel: str) -> bool:
    """检查 STOP 信号文件是否存在。"""
    stop_path = project_root / stop_file_rel
    return stop_path.exists()


def _consume_stop_signal(project_root: Path, stop_file_rel: str) -> None:
    """删除 STOP 信号文件（消费信号，不影响下次启动）。"""
    stop_path = project_root / stop_file_rel
    try:
        if stop_path.exists():
            stop_path.unlink()
    except Exception:
        pass


# ============================================================
# 进度条打印
# ============================================================


def _print_progress(
    total_dialogs: int,
    global_offset: int,
    run_start_offset: int,
    max_dialogs_this_run: int,
    run_dialogs_done: int,
    total_records: int,
    run_elapsed_seconds: float,
    cfg: Dict[str, Any],
) -> None:
    """打印一行可读的进度信息。"""
    # 全局进度
    global_pct = (global_offset / total_dialogs * 100) if total_dialogs > 0 else 0

    # 本次运行进度
    run_pct = (run_dialogs_done / max_dialogs_this_run * 100) if max_dialogs_this_run > 0 else 0

    # 预估本次剩余时间
    run_dialogs_remaining = max_dialogs_this_run - run_dialogs_done
    if run_dialogs_done > 0 and run_elapsed_seconds > 0:
        # 基于实际速度计算
        secs_per_dialog = run_elapsed_seconds / run_dialogs_done
        est_remaining = run_dialogs_remaining * secs_per_dialog
    else:
        # 基于配置估算
        est_per_dialog = cfg["est_instances_per_dialog"] * cfg["est_seconds_per_instance"]
        est_remaining = run_dialogs_remaining * est_per_dialog

    # 进度条（20 字符宽）
    bar_width = 20
    filled = int(bar_width * global_pct / 100)
    bar = "█" * filled + "░" * (bar_width - filled)

    _log(
        f"[进度] {bar} {global_offset}/{total_dialogs} ({global_pct:.1f}%) "
        f"| 本次 {run_dialogs_done}/{max_dialogs_this_run} "
        f"| {total_records:,}条已入库 "
        f"| 剩余~{_format_duration(est_remaining)}"
    )


# ============================================================
# 子进程调用
# ============================================================


def _run_child(
    cmd: List[str], cwd: Path, stage_name: str, dry_run: bool = False,
) -> Dict[str, Any]:
    cmd_str = " ".join(str(c) for c in cmd)

    if dry_run:
        _log(f"  [{stage_name}] [DRY RUN] {cmd_str}")
        return {
            "success": True, "stage": stage_name, "exit_code": 0,
            "stdout_json": {"status": "dry-run", "action": stage_name},
            "stderr_tail": "", "elapsed_seconds": 0.0, "command": cmd_str,
        }

    t0 = time.monotonic()
    result = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8",
        errors="replace", cwd=str(cwd),
    )
    elapsed = round(time.monotonic() - t0, 2)

    # 解析 stdout JSON
    stdout_json = None
    stdout_text = (result.stdout or "").strip()
    if stdout_text:
        try:
            stdout_json = json.loads(stdout_text)
        except json.JSONDecodeError:
            for line in reversed(stdout_text.splitlines()):
                line = line.strip()
                if line.startswith("{"):
                    try:
                        stdout_json = json.loads(line)
                        break
                    except json.JSONDecodeError:
                        continue

    stderr_text = (result.stderr or "").strip()
    stderr_tail = stderr_text[-2000:] if len(stderr_text) > 2000 else stderr_text

    success = result.returncode == 0
    if not success:
        _log(f"  [{stage_name}] 失败 (exit_code={result.returncode})")
        if stderr_tail:
            _log(f"  [{stage_name}] stderr: {stderr_tail[:500]}")

    return {
        "success": success, "stage": stage_name, "exit_code": result.returncode,
        "stdout_json": stdout_json, "stderr_tail": stderr_tail,
        "elapsed_seconds": elapsed, "command": cmd_str,
    }


# ============================================================
# 产物移动与清理
# ============================================================


def _move_jsonl_preserve_meta(source_dir: Path, target_dir: Path, stage_name: str) -> Dict[str, Any]:
    if not source_dir.exists():
        return {"status": "skipped", "reason": f"source not found: {source_dir}"}

    _ensure_dir(target_dir)
    moved_files = 0
    preserved_meta = False

    for f in source_dir.iterdir():
        if f.is_file():
            if f.name == "run_meta.json":
                preserved_meta = True
                continue
            shutil.move(str(f), str(target_dir / f.name))
            moved_files += 1

    for subdir in list(source_dir.iterdir()):
        if subdir.is_dir():
            target_subdir = target_dir / subdir.name
            _ensure_dir(target_subdir)
            for f in subdir.iterdir():
                if f.is_file():
                    shutil.move(str(f), str(target_subdir / f.name))
                    moved_files += 1
            if not any(subdir.iterdir()):
                subdir.rmdir()

    return {"status": "ok", "stage": stage_name, "moved_files": moved_files, "preserved_meta": preserved_meta}


def _cleanup_final_jsonl(directory: Path, stage_name: str) -> Dict[str, Any]:
    if not directory.exists():
        return {"status": "skipped"}

    removed = 0
    for jsonl_file in directory.rglob("*.jsonl"):
        jsonl_file.unlink()
        removed += 1

    for subdir in sorted(directory.rglob("*"), reverse=True):
        if subdir.is_dir() and not any(subdir.iterdir()):
            subdir.rmdir()

    return {"status": "ok", "stage": stage_name, "jsonl_removed": removed}


def _check_data_exhausted(stdout_json: Optional[Dict[str, Any]]) -> bool:
    if stdout_json is None:
        return False
    if stdout_json.get("status") == "info":
        if "No matching records found" in stdout_json.get("message", ""):
            return True
    if stdout_json.get("status") == "success" and stdout_json.get("total_generated", -1) == 0:
        return True
    return False


# ============================================================
# 单批次流水线
# ============================================================


def _run_single_batch(
    project_root: Path, batch_id: str,
    batch_size: int, offset: int,
    mock_api: bool, dry_run: bool,
) -> Dict[str, Any]:
    batch_result: Dict[str, Any] = {
        "batch_id": batch_id, "offset": offset, "batch_size": batch_size,
        "status": "started", "stages": {}, "moves": {}, "cleanup": {},
        "started_at": _utc_iso(), "error": None, "data_exhausted": False,
        "records_generated": 0,
    }

    abs_pipeline = project_root / PIPELINE_BASE
    dir_1 = abs_pipeline / STAGE_1_GENERATED / batch_id
    dir_2 = abs_pipeline / STAGE_2_VALIDATED / batch_id
    dir_3 = abs_pipeline / STAGE_3_LIFECYCLE / batch_id

    # ── Generator ──
    gen_cmd = [
        sys.executable, str(project_root / SCRIPT_GENERATOR),
        "--run-id", batch_id, "--target", "instance", "--all-assets",
        "--limit-dialogs", str(batch_size), "--offset-dialogs", str(offset),
    ]
    if mock_api:
        gen_cmd.append("--mock-api")

    gen_result = _run_child(gen_cmd, project_root, "generator", dry_run=dry_run)
    batch_result["stages"]["generator"] = gen_result

    if _check_data_exhausted(gen_result.get("stdout_json")):
        batch_result["status"] = "data_exhausted"
        batch_result["data_exhausted"] = True
        batch_result["finished_at"] = _utc_iso()
        return batch_result

    if not gen_result["success"]:
        batch_result["status"] = "failed"
        batch_result["error"] = f"generator failed (exit_code={gen_result['exit_code']})"
        batch_result["finished_at"] = _utc_iso()
        return batch_result

    gen_json = gen_result.get("stdout_json") or {}
    batch_result["records_generated"] = gen_json.get("total_generated", 0)

    # ── Validator ──
    val_cmd = [sys.executable, str(project_root / SCRIPT_VALIDATOR), "--input-dir", str(dir_1)]
    val_result = _run_child(val_cmd, project_root, "validator", dry_run=dry_run)
    batch_result["stages"]["validator"] = val_result

    if not val_result["success"]:
        batch_result["status"] = "failed"
        batch_result["error"] = f"validator failed (exit_code={val_result['exit_code']})"
        batch_result["finished_at"] = _utc_iso()
        return batch_result

    if not dry_run:
        batch_result["moves"]["1_to_2"] = _move_jsonl_preserve_meta(dir_1, dir_2, "1→2")

    # ── Lifecycle Guard ──
    life_cmd = [
        sys.executable, str(project_root / SCRIPT_LIFECYCLE),
        "--input-dir", str(dir_2), "--output-dir", str(dir_3),
    ]
    life_result = _run_child(life_cmd, project_root, "lifecycle", dry_run=dry_run)
    batch_result["stages"]["lifecycle"] = life_result

    if not life_result["success"]:
        batch_result["status"] = "failed"
        batch_result["error"] = f"lifecycle failed (exit_code={life_result['exit_code']})"
        batch_result["finished_at"] = _utc_iso()
        return batch_result

    if not dry_run:
        batch_result["cleanup"]["2_validated"] = _cleanup_final_jsonl(dir_2, "cleanup_2")

    # ── Writer ──
    write_cmd = [sys.executable, str(project_root / SCRIPT_WRITER), "--input-dir", str(dir_3)]
    write_result = _run_child(write_cmd, project_root, "writer", dry_run=dry_run)
    batch_result["stages"]["writer"] = write_result

    if not write_result["success"]:
        batch_result["status"] = "failed"
        batch_result["error"] = f"writer failed (exit_code={write_result['exit_code']})"
        batch_result["finished_at"] = _utc_iso()
        return batch_result

    if not dry_run:
        batch_result["cleanup"]["3_lifecycle"] = _cleanup_final_jsonl(dir_3, "cleanup_3")

    batch_result["status"] = "success"
    batch_result["finished_at"] = _utc_iso()
    return batch_result


# ============================================================
# 留档写入
# ============================================================


def _write_checkpoint(
    project_root: Path, runner_run_id: str,
    start_offset: int, batch_size: int,
    total_batches_success: int, total_records: int,
    started_at: str, mock_api: bool, dry_run: bool,
    stop_reason: Optional[str], batch_results: List[Dict[str, Any]],
) -> None:
    try:
        log_dir = project_root / PIPELINE_BASE / LOG_DIR_NAME
        _ensure_dir(log_dir)
        meta = {
            "script_name": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "runner_run_id": runner_run_id,
            "target": "instance",
            "start_offset": start_offset,
            "batch_size": batch_size,
            "total_batches_success": total_batches_success,
            "total_records_generated": total_records,
            "next_offset": start_offset + total_batches_success * batch_size,
            "mock_api": mock_api,
            "dry_run": dry_run,
            "stop_reason": stop_reason,
            "started_at": started_at,
            "updated_at": _utc_iso(),
            "batches": batch_results,
        }
        meta_file = log_dir / f"{runner_run_id}.json"
        meta_file.write_text(_pretty_json(meta), encoding="utf-8")
    except Exception as e:
        _log(f"[警告] 留档写入失败: {e}")


# ============================================================
# 调度主逻辑
# ============================================================


def run(project_root: Path, mock_api: bool, dry_run: bool) -> Dict[str, Any]:
    runner_started = _utc_iso()
    run_t0 = time.monotonic()
    runner_run_id = f"instance_run_{_timestamp_for_id()}"

    # ── 读取 config ──
    cfg = _load_config(project_root)
    dialogs_per_batch = cfg["dialogs_per_batch"]
    max_dialogs = cfg["max_dialogs_per_run"]
    total_dialogs = cfg["total_dialogs"]
    stop_file_rel = cfg["stop_file"]

    # ── 检测进度 ──
    start_offset = _detect_progress(project_root)
    remaining_global = max(0, total_dialogs - start_offset)
    # 本次实际要跑的对话数 = min(阈值, 剩余)
    dialogs_this_run = min(max_dialogs, remaining_global)

    # ── 启动信息 ──
    _log(f"run_id: {runner_run_id}")
    if dry_run:
        _log("*** DRY RUN 模式 ***")
    if mock_api:
        _log("*** MOCK API 模式 ***")

    _log("")
    _log("=" * 60)
    if start_offset == 0:
        _log("首次运行，从头开始。")
    else:
        _log(f"上次进度: {start_offset}/{total_dialogs} 个对话已完成。")

    if remaining_global <= 0:
        _log("所有 instance 已向量化完毕！无需运行。")
        _log("=" * 60)
        return {"script_name": SCRIPT_NAME, "status": "already_complete",
                "start_offset": start_offset, "total_dialogs": total_dialogs}

    est_instances = dialogs_this_run * cfg["est_instances_per_dialog"]
    est_time = est_instances * cfg["est_seconds_per_instance"]
    est_storage = est_instances * cfg["est_bytes_per_instance"]

    _log(f"本次计划: {dialogs_this_run} 个对话 (阈值={max_dialogs})")
    _log(f"预估:     ~{est_instances:,.0f} 条, ~{_format_duration(est_time)}, ~{_format_size(est_storage)}")
    _log(f"信号文件: {stop_file_rel} (echo stop > {stop_file_rel} 可优雅停止)")
    _log("=" * 60)

    # ── 跑批次 ──
    batch_results: List[Dict[str, Any]] = []
    total_batches_success = 0
    total_records = 0
    run_dialogs_done = 0
    stop_reason: Optional[str] = None
    batch_index = 0

    try:
        while run_dialogs_done < dialogs_this_run:
            current_offset = start_offset + batch_index * dialogs_per_batch

            # 边界保护
            if current_offset >= total_dialogs:
                stop_reason = "all_dialogs_covered"
                _log("已覆盖所有对话。")
                break

            batch_id = f"inst_{_timestamp_for_id()}_{batch_index:04d}"

            _log(f"\n--- 批次 {batch_index + 1}: 对话 #{current_offset}~#{current_offset + dialogs_per_batch - 1} ---")

            batch_result = _run_single_batch(
                project_root=project_root,
                batch_id=batch_id,
                batch_size=dialogs_per_batch,
                offset=current_offset,
                mock_api=mock_api,
                dry_run=dry_run,
            )

            batch_results.append(batch_result)
            batch_index += 1

            if batch_result["status"] == "success":
                total_batches_success += 1
                total_records += batch_result.get("records_generated", 0)
                run_dialogs_done += dialogs_per_batch

                # 立即写留档
                if not dry_run:
                    _write_checkpoint(
                        project_root, runner_run_id, start_offset,
                        dialogs_per_batch, total_batches_success, total_records,
                        runner_started, mock_api, dry_run,
                        stop_reason=None, batch_results=batch_results,
                    )

                # 打印进度
                global_offset = start_offset + run_dialogs_done
                _print_progress(
                    total_dialogs=total_dialogs,
                    global_offset=global_offset,
                    run_start_offset=start_offset,
                    max_dialogs_this_run=dialogs_this_run,
                    run_dialogs_done=run_dialogs_done,
                    total_records=total_records,
                    run_elapsed_seconds=time.monotonic() - run_t0,
                    cfg=cfg,
                )

                # 检查信号文件
                if _check_stop_signal(project_root, stop_file_rel):
                    stop_reason = "stop_signal"
                    _consume_stop_signal(project_root, stop_file_rel)
                    _log("检测到 STOP 信号文件，优雅停止。")
                    break

            elif batch_result["status"] == "data_exhausted":
                stop_reason = "data_exhausted"
                _log("所有 instance 数据已处理完毕！")
                break

            elif batch_result["status"] == "failed":
                stop_reason = f"batch_failed: {batch_result.get('error', 'unknown')}"
                _log(f"批次失败: {batch_result.get('error')}")
                break

        else:
            # while 正常退出 → 达到阈值
            stop_reason = "threshold_reached"
            _log(f"达到本次运行阈值 ({dialogs_this_run} 个对话)，自动停止。")

    except KeyboardInterrupt:
        stop_reason = "user_interrupted"
        _log("\n用户中断 (Ctrl+C)。")
        _log("当前批次未完成，不计入进度。下次启动自动续跑。")

    # ── 结束 ──
    runner_finished = _utc_iso()
    next_offset = start_offset + total_batches_success * dialogs_per_batch

    overall_status = "success"
    if any(b["status"] == "failed" for b in batch_results):
        overall_status = "partial_failure" if total_batches_success > 0 else "failed"
    if stop_reason == "user_interrupted":
        overall_status = "interrupted"

    runner_meta = {
        "script_name": SCRIPT_NAME, "script_version": SCRIPT_VERSION,
        "main_layer": MAIN_LAYER, "runner_run_id": runner_run_id,
        "target": "instance",
        "start_offset": start_offset, "batch_size": dialogs_per_batch,
        "max_dialogs_per_run": max_dialogs,
        "total_batches_success": total_batches_success,
        "total_records_generated": total_records,
        "next_offset": next_offset,
        "mock_api": mock_api, "dry_run": dry_run,
        "status": overall_status, "stop_reason": stop_reason,
        "started_at": runner_started, "finished_at": runner_finished,
        "batches": batch_results,
    }

    # 最终留档
    if not dry_run:
        _write_checkpoint(
            project_root, runner_run_id, start_offset,
            dialogs_per_batch, total_batches_success, total_records,
            runner_started, mock_api, dry_run,
            stop_reason=stop_reason, batch_results=batch_results,
        )

    # 结束汇报
    _log("")
    _log("=" * 60)
    _log(f"本次: {total_batches_success} 批, {total_records:,} 条记录")
    _log(f"进度: {next_offset}/{total_dialogs} ({next_offset/total_dialogs*100:.1f}%)")
    remaining_after = max(0, total_dialogs - next_offset)
    if remaining_after > 0:
        _log(f"剩余: {remaining_after} 个对话")
        _log(f"下次直接运行同一命令即可。")
    else:
        _log("全部 instance 向量化完成！")
    _log("=" * 60)

    return runner_meta


# ============================================================
# CLI / main
# ============================================================


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Instance 向量化：开了就跑，关了就停，下次开了接着跑。",
    )
    ap.add_argument("--dry-run", action="store_true", help="预览模式。")
    ap.add_argument("--mock-api", action="store_true", help="测试用随机向量。")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    here = Path(__file__).resolve()
    project_root = _find_project_root(here)

    result = run(
        project_root=project_root,
        mock_api=args.mock_api,
        dry_run=args.dry_run,
    )

    print(_pretty_json(result))
    return 1 if result.get("status") == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
