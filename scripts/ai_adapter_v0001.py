#!/usr/bin/env python3
# ============================================================
# File: ai_adapter_v0001.py
# 中文名: 理解端 AI Adapter 脚本
# Version: v0001
# Layer: adapter
# Main Layer: understanding
# Updatable: True
#
# Purpose
# 本脚本为理解端提供 AI 能力的制度化接入与调用治理入口
# 所有 AI 能力必须先注册为 capability alias
# 所有 AI 调用必须通过 adapter 的统一入口并受全局开关控制
#
# What it does
# 1) 管理理解端 AI capability 注册表（静态制度事实）
# 2) 管理理解端 AI 全局开关（默认关闭）
# 3) 提供统一的 call 入口用于“放行/拒绝”决策与结构化输出
# 4) v0001 不执行真实 AI 调用，仅输出可登记的调用裁决结果
#
# What it does NOT do
# 1) 不直接调用任何外部 AI API
# 2) 不实现 agent 路由或多模型调度
# 3) 不解析 prompt 语义，不评估输出质量
# 4) 不写入理解端登记系统，不记录使用事实
# 5) 不自动选择替代模型或 provider
#
# Non-v0001 targets (deferred, still recorded here)
# 1) 接入真实 API 调用与 usage summary 结构输出
# 2) 支持本地模型与 agent wrapper 的统一调用协议
# 3) 增加调用审计日志 jsonl 与回滚工具链
# 4) 细化 capability schema 与 validator 家族联动
# 5) 成本控制 速率限制 重试策略 缓存策略
#
# Notes
# - AI adapter 是门面与闸门，不是智能本体
# - 理解端登记脚本负责登记“是否使用/使用了什么”，adapter 只输出裁决结构
# - 全局开关默认关闭，必须显式启用
# ============================================================

# ===========================
# ALIAS_META (comment block)
# ===========================
# alias: ai_adapter
# family: understanding_ai_adapter
# role: understanding_ai_gateway
# version: v0001
# status: active
# entry_point: ai_adapter_v0001.py
# input:
#   - command add|get|list|update|remove|show-switch|set-switch|call
#   - capability alias
#   - provider mode default_model status
#   - ai_enabled global switch
#   - payload json (for call)
# output:
#   - json summary to stdout
# depends_on: []
# used_by:
#   - understanding flows (preflight)
#   - understanding analysis scripts (via call)

# =====================================
# 制度与职责说明注释区
# =====================================
# - 本脚本管理理解端 AI capability 的静态制度事实与全局开关
# - capability 注册表与第三方库注册表类似，但对象是 AI 能力而不是 pip 包
# - 本脚本不执行真实 AI 调用，v0001 仅输出“允许/拒绝”裁决结构
# - 本脚本不得写入理解端登记记录，登记由 register_understanding_lineage 脚本承担
# - 全局开关默认关闭，禁用时必须明确拒绝，不允许静默放行

from __future__ import annotations

# =====================================
# Imports 区
# =====================================
import argparse
import json
import platform
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
# - 不做任何网络请求
# - 不读取或使用 API key
# - 写入采用“临时文件 + replace”减少中断风险
# - dry-run 下不得写入或删除任何文件

# =====================================
# 常量与全局配置区
# =====================================
SCRIPT_NAME = "ai_adapter_v0001.py"
SCRIPT_VERSION = "v0001"

DEFAULT_CAP_REGISTRY_DIR = Path("config/understanding/ai/capabilities")
DEFAULT_SETTINGS_PATH = Path("config/understanding/ai/settings.json")
DEFAULT_CALL_LOG_DIR = Path("config/understanding/ai/call_logs")

UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"
ALIAS_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,63}$")

CAP_STATUS_ENABLED = "enabled"
CAP_STATUS_DISABLED = "disabled"

MODE_API = "api"
MODE_LOCAL = "local"
MODE_AGENT = "agent"
MODE_NONE = "none"

ALLOWED_MODES = {MODE_API, MODE_LOCAL, MODE_AGENT, MODE_NONE}
ALLOWED_CAP_STATUS = {CAP_STATUS_ENABLED, CAP_STATUS_DISABLED}


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


def validate_alias(alias: str) -> None:
    if not ALIAS_PATTERN.match(alias):
        raise ValueError("alias 不合法 需要小写字母开头 仅包含小写字母 数字 下划线 长度 2 到 64")


def cap_path_for_alias(cap_registry_dir: Path, alias: str) -> Path:
    return cap_registry_dir / f"{alias}.json"


def ensure_file_exists(path: Path, alias: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"alias 未注册或文件不存在 {alias} path {path}")


def ensure_file_not_exists(path: Path, alias: str) -> None:
    if path.exists():
        raise FileExistsError(f"alias 已存在 禁止覆盖 {alias} path {path}")


def env_snapshot() -> Dict[str, str]:
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "executable": sys.executable,
    }


def minimal_cap_summary(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "alias": rec.get("alias"),
        "provider": rec.get("provider"),
        "mode": rec.get("mode"),
        "default_model": rec.get("default_model"),
        "status": rec.get("status"),
    }


def get_next_call_id(call_log_dir: Path) -> str:
    """获取下一个递增的 call_id"""
    call_log_dir.mkdir(parents=True, exist_ok=True)
    
    existing = list(call_log_dir.glob("call_*.json"))
    if not existing:
        return "call_000001"
    
    max_num = 0
    for f in existing:
        # call_000123.json → 123
        num_str = f.stem.replace("call_", "")
        try:
            num = int(num_str)
            max_num = max(max_num, num)
        except ValueError:
            continue
    
    next_num = max_num + 1
    return f"call_{str(next_num).zfill(6)}"


def write_call_log(
    call_log_dir: Path,
    call_id: str,
    decision: Dict[str, Any],
    payload: Dict[str, Any],
) -> Path:
    """写入 call 裁决日志"""
    call_log_dir.mkdir(parents=True, exist_ok=True)
    
    log_record = {
        "call_id": call_id,
        "timestamp": utc_now(),
        "capability": decision.get("capability"),
        "allowed": decision.get("allowed"),
        "reason": decision.get("reason"),
        "provider": decision.get("provider"),
        "mode": decision.get("mode"),
        "model": decision.get("model"),
        "payload": payload,
    }
    
    log_path = call_log_dir / f"{call_id}.json"
    atomic_write_json(log_path, log_record)
    
    return log_path


def parse_payload(payload_raw: Optional[str], payload_file: Optional[Path]) -> Dict[str, Any]:
    if payload_raw and payload_file:
        raise ValueError("不得同时使用 --payload 与 --payload-file")
    if payload_file:
        data = load_json(payload_file)
        if not isinstance(data, dict):
            raise ValueError("payload-file 必须为 JSON object")
        return data
    if payload_raw:
        data = json.loads(payload_raw)
        if not isinstance(data, dict):
            raise ValueError("payload 必须为 JSON object")
        return data
    return {}


def normalize_mode(mode: str) -> str:
    m = (mode or "").strip().lower()
    if m not in ALLOWED_MODES:
        raise ValueError(f"mode 不支持 允许值 {sorted(ALLOWED_MODES)}")
    return m


def normalize_cap_status(status: str) -> str:
    s = (status or "").strip().lower()
    if s not in ALLOWED_CAP_STATUS:
        raise ValueError(f"status 不支持 允许值 {sorted(ALLOWED_CAP_STATUS)}")
    return s


def read_settings(settings_path: Path) -> Dict[str, Any]:
    if settings_path.exists():
        data = load_json(settings_path)
        if not isinstance(data, dict):
            raise ValueError("settings.json 必须为 JSON object")
        return data
    return {
        "ai_enabled": False,
        "updated_at": utc_now(),
        "updated_by": SCRIPT_NAME,
    }


def write_settings(settings_path: Path, settings: Dict[str, Any], dry_run: bool) -> None:
    settings["updated_at"] = utc_now()
    settings["updated_by"] = SCRIPT_NAME
    if not dry_run:
        atomic_write_json(settings_path, settings)


# =====================================
# 核心业务逻辑区
# =====================================
@dataclass(frozen=True)
class CommandResult:
    status: str
    action: str
    dry_run: bool
    details: Dict[str, Any]


def cmd_add_capability(
    *,
    cap_registry_dir: Path,
    alias: str,
    provider: str,
    mode: str,
    default_model: str,
    status: str,
    dry_run: bool,
) -> CommandResult:
    validate_alias(alias)
    path = cap_path_for_alias(cap_registry_dir, alias)
    ensure_file_not_exists(path, alias)

    rec: Dict[str, Any] = {
        "alias": alias,
        "provider": (provider or "").strip(),
        "mode": normalize_mode(mode),
        "default_model": (default_model or "").strip(),
        "status": normalize_cap_status(status),
        "registered_at": utc_now(),
        "registered_by": SCRIPT_NAME,
    }

    if not rec["provider"]:
        raise ValueError("provider 不得为空")
    if not rec["default_model"] and rec["mode"] in {MODE_API, MODE_LOCAL, MODE_AGENT}:
        # v0001 允许为空但不推荐 这里做最小约束以防制度空洞
        raise ValueError("default_model 不得为空")

    if not dry_run:
        atomic_write_json(path, rec)

    return CommandResult(
        status="ok",
        action="add",
        dry_run=dry_run,
        details={
            "capability_path": str(path),
            "capability": rec,
        },
    )


def cmd_get_capability(*, cap_registry_dir: Path, alias: str) -> CommandResult:
    validate_alias(alias)
    path = cap_path_for_alias(cap_registry_dir, alias)
    ensure_file_exists(path, alias)
    rec = load_json(path)

    return CommandResult(
        status="ok",
        action="get",
        dry_run=False,
        details={
            "capability_path": str(path),
            "capability": rec,
        },
    )


def cmd_list_capabilities(*, cap_registry_dir: Path) -> CommandResult:
    cap_registry_dir.mkdir(parents=True, exist_ok=True)
    items: List[Dict[str, Any]] = []
    for p in sorted(cap_registry_dir.glob("*.json")):
        try:
            rec = load_json(p)
            items.append(minimal_cap_summary(rec))
        except Exception as e:
            items.append(
                {
                    "alias": p.stem,
                    "provider": None,
                    "mode": None,
                    "default_model": None,
                    "status": "error",
                    "error": f"failed_to_read {type(e).__name__}",
                    "path": str(p),
                }
            )

    return CommandResult(
        status="ok",
        action="list",
        dry_run=False,
        details={
            "count": len(items),
            "items": items,
        },
    )


def cmd_update_capability(
    *,
    cap_registry_dir: Path,
    alias: str,
    provider: Optional[str],
    mode: Optional[str],
    default_model: Optional[str],
    status: Optional[str],
    dry_run: bool,
) -> CommandResult:
    validate_alias(alias)
    path = cap_path_for_alias(cap_registry_dir, alias)
    ensure_file_exists(path, alias)
    rec = load_json(path)
    before = dict(rec)

    # 不允许修改 alias / registered_at / registered_by
    if rec.get("alias") != alias:
        raise ValueError("制度违规 记录 alias 与文件 alias 不一致")

    if provider is not None:
        rec["provider"] = provider.strip()
    if mode is not None:
        rec["mode"] = normalize_mode(mode)
    if default_model is not None:
        rec["default_model"] = default_model.strip()
    if status is not None:
        rec["status"] = normalize_cap_status(status)

    if not rec.get("provider"):
        raise ValueError("provider 不得为空")
    if not rec.get("default_model") and rec.get("mode") in {MODE_API, MODE_LOCAL, MODE_AGENT}:
        raise ValueError("default_model 不得为空")

    if not dry_run:
        atomic_write_json(path, rec)

    return CommandResult(
        status="ok",
        action="update",
        dry_run=dry_run,
        details={
            "capability_path": str(path),
            "before": before,
            "after": rec,
        },
    )


def cmd_remove_capability(*, cap_registry_dir: Path, alias: str, dry_run: bool) -> CommandResult:
    validate_alias(alias)
    path = cap_path_for_alias(cap_registry_dir, alias)
    ensure_file_exists(path, alias)
    rec = load_json(path)

    if not dry_run:
        path.unlink()

    return CommandResult(
        status="ok",
        action="remove",
        dry_run=dry_run,
        details={
            "capability_path": str(path),
            "removed_capability": rec,
        },
    )


def cmd_show_switch(*, settings_path: Path) -> CommandResult:
    settings = read_settings(settings_path)
    return CommandResult(
        status="ok",
        action="show-switch",
        dry_run=False,
        details={
            "settings_path": str(settings_path),
            "settings": settings,
        },
    )


def cmd_set_switch(*, settings_path: Path, enabled: bool, dry_run: bool) -> CommandResult:
    settings = read_settings(settings_path)
    before = dict(settings)
    settings["ai_enabled"] = bool(enabled)

    write_settings(settings_path, settings, dry_run=dry_run)

    return CommandResult(
        status="ok",
        action="set-switch",
        dry_run=dry_run,
        details={
            "settings_path": str(settings_path),
            "before": before,
            "after": settings,
        },
    )


def cmd_call(
    *,
    settings_path: Path,
    cap_registry_dir: Path,
    call_log_dir: Path,
    capability: str,
    payload: Dict[str, Any],
    dry_run: bool = False,
) -> CommandResult:
    validate_alias(capability)

    settings = read_settings(settings_path)
    ai_enabled = bool(settings.get("ai_enabled", False))

    cap_path = cap_path_for_alias(cap_registry_dir, capability)
    if not cap_path.exists():
        # call 视为运行期请求 但 alias 未注册属于制度错误
        raise FileNotFoundError(f"制度错误 capability 未注册 {capability} path {cap_path}")

    cap_rec = load_json(cap_path)

    cap_status = str(cap_rec.get("status") or "").strip().lower()
    provider = cap_rec.get("provider")
    mode = cap_rec.get("mode")
    model = cap_rec.get("default_model")

    # 生成 call_id
    call_id = get_next_call_id(call_log_dir)

    if not ai_enabled:
        decision = {
            "allowed": False,
            "reason": "ai_disabled",
            "capability": capability,
            "provider": None,
            "mode": None,
            "model": None,
            "executed": False,
        }
        
        # 留档（即使拒绝也要记录）
        log_path = None
        if not dry_run:
            log_path = write_call_log(call_log_dir, call_id, decision, payload)
        
        return CommandResult(
            status="ok",
            action="call",
            dry_run=dry_run,
            details={
                "call_id": call_id,
                "decision": decision,
                "environment": env_snapshot(),
                "settings_path": str(settings_path),
                "capability_path": str(cap_path),
                "log_path": str(log_path) if log_path else None,
                "payload": payload,
            },
        )

    if cap_status != CAP_STATUS_ENABLED:
        decision = {
            "allowed": False,
            "reason": "capability_disabled",
            "capability": capability,
            "provider": provider,
            "mode": mode,
            "model": model,
            "executed": False,
        }
        
        # 留档
        log_path = None
        if not dry_run:
            log_path = write_call_log(call_log_dir, call_id, decision, payload)
        
        return CommandResult(
            status="ok",
            action="call",
            dry_run=dry_run,
            details={
                "call_id": call_id,
                "decision": decision,
                "environment": env_snapshot(),
                "settings_path": str(settings_path),
                "capability_path": str(cap_path),
                "log_path": str(log_path) if log_path else None,
                "payload": payload,
            },
        )

    # v0001 不执行真实调用 只返回放行裁决
    decision = {
        "allowed": True,
        "reason": None,
        "capability": capability,
        "provider": provider,
        "mode": mode,
        "model": model,
        "executed": False,
        "note": "call_not_executed_in_v0001",
    }

    # 留档
    log_path = None
    if not dry_run:
        log_path = write_call_log(call_log_dir, call_id, decision, payload)

    return CommandResult(
        status="ok",
        action="call",
        dry_run=dry_run,
        details={
            "call_id": call_id,
            "decision": decision,
            "environment": env_snapshot(),
            "settings_path": str(settings_path),
            "capability_path": str(cap_path),
            "log_path": str(log_path) if log_path else None,
            "payload": payload,
        },
    )


# =====================================
# CLI / main 接口区
# =====================================
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Understanding-layer AI adapter (gateway + registry).")

    parser.add_argument(
        "--cap-registry-dir",
        default=str(DEFAULT_CAP_REGISTRY_DIR),
        help="Capability registry dir. Default: config/understanding/ai/capabilities",
    )
    parser.add_argument(
        "--settings-path",
        default=str(DEFAULT_SETTINGS_PATH),
        help="AI settings path. Default: config/understanding/ai/settings.json",
    )
    parser.add_argument(
        "--call-log-dir",
        default=str(DEFAULT_CALL_LOG_DIR),
        help="Call log directory. Default: config/understanding/ai/call_logs",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_add = sub.add_parser("add", help="Add a new AI capability (one alias one file).")
    p_add.add_argument("--alias", required=True)
    p_add.add_argument("--provider", required=True)
    p_add.add_argument("--mode", required=True, help="api|local|agent|none")
    p_add.add_argument("--default-model", required=True, dest="default_model")
    p_add.add_argument("--status", default=CAP_STATUS_DISABLED, help="enabled|disabled (default disabled)")
    p_add.add_argument("--dry-run", action="store_true")

    p_get = sub.add_parser("get", help="Get a capability record by alias.")
    p_get.add_argument("--alias", required=True)

    sub.add_parser("list", help="List all capability records (summary).")

    p_up = sub.add_parser("update", help="Update mutable fields of an existing capability.")
    p_up.add_argument("--alias", required=True)
    p_up.add_argument("--provider")
    p_up.add_argument("--mode", help="api|local|agent|none")
    p_up.add_argument("--default-model", dest="default_model")
    p_up.add_argument("--status", help="enabled|disabled")
    p_up.add_argument("--dry-run", action="store_true")

    p_rm = sub.add_parser("remove", help="Remove a capability record by alias.")
    p_rm.add_argument("--alias", required=True)
    p_rm.add_argument("--dry-run", action="store_true")

    sub.add_parser("show-switch", help="Show global AI switch settings.")

    p_set = sub.add_parser("set-switch", help="Set global AI switch (default is disabled).")
    p_set.add_argument("--enabled", required=True, choices=["true", "false"])
    p_set.add_argument("--dry-run", action="store_true")

    p_call = sub.add_parser("call", help="Request an AI call decision via adapter (v0001 does not execute).")
    p_call.add_argument("--capability", required=True)
    p_call.add_argument("--payload", help="JSON object string")
    p_call.add_argument("--payload-file", help="JSON object file path")
    p_call.add_argument("--dry-run", action="store_true", help="Do not write call log")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    cap_registry_dir = Path(args.cap_registry_dir)
    settings_path = Path(args.settings_path)

    try:
        if args.command == "add":
            res = cmd_add_capability(
                cap_registry_dir=cap_registry_dir,
                alias=args.alias,
                provider=args.provider,
                mode=args.mode,
                default_model=args.default_model,
                status=args.status,
                dry_run=bool(args.dry_run),
            )
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "get":
            res = cmd_get_capability(cap_registry_dir=cap_registry_dir, alias=args.alias)
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "list":
            res = cmd_list_capabilities(cap_registry_dir=cap_registry_dir)
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "update":
            res = cmd_update_capability(
                cap_registry_dir=cap_registry_dir,
                alias=args.alias,
                provider=args.provider,
                mode=args.mode,
                default_model=args.default_model,
                status=args.status,
                dry_run=bool(args.dry_run),
            )
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "remove":
            res = cmd_remove_capability(
                cap_registry_dir=cap_registry_dir,
                alias=args.alias,
                dry_run=bool(args.dry_run),
            )
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "show-switch":
            res = cmd_show_switch(settings_path=settings_path)
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "set-switch":
            enabled = True if args.enabled == "true" else False
            res = cmd_set_switch(settings_path=settings_path, enabled=enabled, dry_run=bool(args.dry_run))
            print(dumps_json(res.__dict__))
            return 0

        if args.command == "call":
            payload_file = Path(args.payload_file) if args.payload_file else None
            payload = parse_payload(args.payload, payload_file)
            call_log_dir = Path(args.call_log_dir)
            res = cmd_call(
                settings_path=settings_path,
                cap_registry_dir=cap_registry_dir,
                call_log_dir=call_log_dir,
                capability=args.capability,
                payload=payload,
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
            "cap_registry_dir": str(cap_registry_dir),
            "settings_path": str(settings_path),
        }
        print(dumps_json(err))
        return 1


# =====================================
# Entry Point
# =====================================
if __name__ == "__main__":
    raise SystemExit(main())