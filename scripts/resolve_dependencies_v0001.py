#!/usr/bin/env python3
# ============================================================
# File: resolve_dependencies_v0001.py
# 中文名: 理解端第三方依赖解析脚本
# Version: v0001
# Layer: runtime_support
# Main Layer: understanding
# Updatable: True
#
# Purpose
# 本脚本用于在理解端运行前解析第三方依赖
# 其目标是基于已注册的理解端第三方能力清单
# 判断当前运行环境中依赖是否可用
# 并输出结构化结果用于后续决策
#
# What it does
# 1) 读取理解端第三方能力注册表目录 config/understanding/dependencies
# 2) 解析依赖输入 alias 列表
# 3) 对每个 alias 查注册信息 package_name 与 version_spec
# 4) 检查当前环境是否已安装并尝试获取版本
# 5) 进行最小版本约束判断并输出 resolved missing unsatisfied
# 6) 可选支持 auto-install 但必须显式开启
# 7) 支持 dry-run 用于仅报告拟安装与拟结果
#
# What it does NOT do
# 1) 不修改第三方能力注册表
# 2) 不新增或删除任何 alias
# 3) 不记录运行期使用事实
# 4) 不进入理解端登记系统
# 5) 不自动选择替代库
# 6) 不扫描或推断脚本源码中的 import
#
# Non-v0001 targets (deferred, still recorded here)
# 1) 解析脚本 Comment Header 或 ALIAS_META 以提取 depends_on_libs
# 2) 更完整的 PEP 440 版本约束解析与组合约束支持
# 3) 跨 layer 依赖联邦解析与一致性检查
# 4) 性能与资源条件感知的依赖选择策略
# 5) 自动替代库选择与策略化回退
#
# Notes
# - 本脚本是运行期解析器
# - 本脚本输出解析事实但不产生制度事实
# - v0001 的版本比较为最小实现仅支持单一约束符
# ============================================================

# ===========================
# ALIAS_META (comment block)
# ===========================
# alias: resolve_dependencies
# family: understanding_dependencies_runtime
# role: understanding_dependency_resolver
# version: v0001
# status: active
# entry_point: resolve_dependencies_v0001.py
# input:
#   - depends aliases via --depends or --depends-file
#   - registry_dir (optional)
#   - auto_install (optional)
#   - dry_run (optional)
#   - fail_fast (optional)
# output:
#   - json result to stdout
# depends_on: []
# used_by:
#   - understanding flows
#   - analysis scripts (as a preflight check)

# =====================================
# 制度与职责说明注释区
# =====================================
# - 本脚本只读取 config/understanding/dependencies 下的静态制度事实
# - 本脚本不得写回注册表
# - 本脚本可选执行环境安装但必须由参数显式开启
# - 本脚本输出仅用于运行前决策
# - 使用事实与审计由理解端登记脚本负责

from __future__ import annotations

# =====================================
# Imports 区
# =====================================
import argparse
import json
import platform
import re
import subprocess
import sys
from dataclasses import dataclass
from importlib import metadata
from importlib.util import find_spec
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# =====================================
# 边界声明与强约束说明
# =====================================
# - 仅使用标准库
# - 不做源码扫描
# - 版本约束解析仅做最小实现
# - auto-install 仅对已注册 alias 生效
# - dry-run 下不得执行安装

# =====================================
# 常量与全局配置区
# =====================================
SCRIPT_NAME = "resolve_dependencies_v0001.py"
SCRIPT_VERSION = "v0001"

DEFAULT_REGISTRY_DIR = Path("config/understanding/dependencies")
ALIAS_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,63}$")

SUPPORTED_OPS = {"==", ">=", "<=", ">", "<"}

# =====================================
# 工具函数区（无副作用）
# =====================================
def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_alias(alias: str) -> None:
    if not ALIAS_PATTERN.match(alias):
        raise ValueError("alias 不合法 需要小写字母开头 仅包含小写字母 数字 下划线 长度 2 到 64")


def split_csv(raw: str) -> List[str]:
    items = [x.strip() for x in raw.split(",")]
    return [x for x in items if x]


def read_dep_aliases(depends: Optional[str], depends_file: Optional[Path]) -> List[str]:
    if depends and depends_file:
        raise ValueError("不得同时使用 --depends 与 --depends-file")

    if depends:
        aliases = split_csv(depends)
    elif depends_file:
        data = load_json(depends_file)
        if isinstance(data, list):
            aliases = [str(x).strip() for x in data if str(x).strip()]
        elif isinstance(data, dict) and "depends" in data and isinstance(data["depends"], list):
            aliases = [str(x).strip() for x in data["depends"] if str(x).strip()]
        else:
            raise ValueError("depends-file 必须为 JSON list 或包含 depends list 的 JSON object")
    else:
        aliases = []

    for a in aliases:
        validate_alias(a)

    # 去重但保持顺序
    seen = set()
    out: List[str] = []
    for a in aliases:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out


def registry_record_path(registry_dir: Path, alias: str) -> Path:
    return registry_dir / f"{alias}.json"


def parse_version_spec(version_spec: str) -> Tuple[str, str]:
    raw = (version_spec or "").strip()
    if not raw:
        return "any", ""

    for op in sorted(SUPPORTED_OPS, key=len, reverse=True):
        if raw.startswith(op):
            return op, raw[len(op):].strip()

    # 最小实现只支持单一约束符
    raise ValueError("version_spec 不支持 当前仅支持 == >= <= > < 形式")


def simple_version_key(v: str) -> Tuple:
    """
    最小实现版本比较
    将版本拆成数字段与非数字段
    该实现不等同于完整 PEP 440
    """
    parts = re.split(r"[^0-9A-Za-z]+", (v or "").strip())
    key: List[Any] = []
    for p in parts:
        if p == "":
            continue
        if p.isdigit():
            key.append(int(p))
        else:
            # 非数字段以字符串处理
            key.append(p.lower())
    return tuple(key)


def compare_versions(installed: str, required: str) -> int:
    a = simple_version_key(installed)
    b = simple_version_key(required)
    if a == b:
        return 0
    return 1 if a > b else -1


def satisfies_version(installed_version: Optional[str], version_spec: str) -> Optional[bool]:
    if not version_spec:
        return True

    op, req = parse_version_spec(version_spec)
    if op == "any":
        return True

    if not installed_version:
        return None

    cmpv = compare_versions(installed_version, req)
    if op == "==":
        return cmpv == 0
    if op == ">=":
        return cmpv >= 0
    if op == "<=":
        return cmpv <= 0
    if op == ">":
        return cmpv > 0
    if op == "<":
        return cmpv < 0

    return None


def env_snapshot() -> Dict[str, str]:
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "executable": sys.executable,
    }


def is_importable(package_name: str) -> bool:
    return find_spec(package_name) is not None


def get_installed_version(package_name: str) -> Optional[str]:
    try:
        return metadata.version(package_name)
    except Exception:
        return None


def pip_install(package_name: str, version_spec: str) -> Tuple[bool, str]:
    """
    返回 success 与 message
    v0001 将 version_spec 直接拼接到 pip 需求字符串
    例如 jieba>=0.42.1
    """
    req = package_name
    vs = (version_spec or "").strip()
    if vs and vs != "any":
        req = f"{package_name}{vs}"

    cmd = [sys.executable, "-m", "pip", "install", req]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode == 0:
            return True, "installed"
        msg = (proc.stderr or proc.stdout or "").strip()
        if not msg:
            msg = "pip install failed"
        return False, msg
    except Exception as e:
        return False, f"{type(e).__name__} {e}"


# =====================================
# 核心业务逻辑区
# =====================================
@dataclass(frozen=True)
class DepCheck:
    alias: str
    package_name: str
    version_spec: str
    installed: bool
    installed_version: Optional[str]
    satisfies: Optional[bool]
    note: Optional[str]


def load_registry_records(registry_dir: Path, aliases: List[str]) -> Dict[str, Dict[str, Any]]:
    records: Dict[str, Dict[str, Any]] = {}
    for alias in aliases:
        path = registry_record_path(registry_dir, alias)
        if not path.exists():
            raise FileNotFoundError(f"制度错误 alias 未注册 {alias} path {path}")
        rec = load_json(path)
        records[alias] = rec
    return records


def check_one(
    *,
    alias: str,
    record: Dict[str, Any],
    auto_install: bool,
    dry_run: bool,
) -> DepCheck:
    package_name = str(record.get("package_name") or "").strip()
    version_spec = str(record.get("version_spec") or "").strip()

    if not package_name:
        return DepCheck(
            alias=alias,
            package_name=package_name,
            version_spec=version_spec,
            installed=False,
            installed_version=None,
            satisfies=None,
            note="invalid_registry_record_missing_package_name",
        )

    installed = is_importable(package_name)
    installed_version = get_installed_version(package_name) if installed else None
    sat = satisfies_version(installed_version, version_spec) if installed else None

    # 若未安装且开启 auto_install 则尝试安装
    if not installed and auto_install:
        if dry_run:
            return DepCheck(
                alias=alias,
                package_name=package_name,
                version_spec=version_spec,
                installed=False,
                installed_version=None,
                satisfies=None,
                note="dry_run_would_install",
            )

        ok, msg = pip_install(package_name, version_spec)
        if ok:
            installed = is_importable(package_name)
            installed_version = get_installed_version(package_name) if installed else None
            sat = satisfies_version(installed_version, version_spec) if installed else None
            return DepCheck(
                alias=alias,
                package_name=package_name,
                version_spec=version_spec,
                installed=installed,
                installed_version=installed_version,
                satisfies=sat,
                note="auto_installed",
            )

        return DepCheck(
            alias=alias,
            package_name=package_name,
            version_spec=version_spec,
            installed=False,
            installed_version=None,
            satisfies=None,
            note=f"auto_install_failed {msg}",
        )

    # 已安装但不满足版本
    if installed and sat is False:
        return DepCheck(
            alias=alias,
            package_name=package_name,
            version_spec=version_spec,
            installed=True,
            installed_version=installed_version,
            satisfies=False,
            note="version_unsatisfied",
        )

    # 已安装且满足
    if installed and sat is True:
        return DepCheck(
            alias=alias,
            package_name=package_name,
            version_spec=version_spec,
            installed=True,
            installed_version=installed_version,
            satisfies=True,
            note=None,
        )

    # 未安装且未开启 auto_install
    return DepCheck(
        alias=alias,
        package_name=package_name,
        version_spec=version_spec,
        installed=False,
        installed_version=None,
        satisfies=None,
        note="missing",
    )


def run_check(
    *,
    registry_dir: Path,
    aliases: List[str],
    auto_install: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    registry_dir.mkdir(parents=True, exist_ok=True)

    records = load_registry_records(registry_dir, aliases)

    resolved: Dict[str, Any] = {}
    missing: Dict[str, Any] = {}
    unsatisfied: Dict[str, Any] = {}

    for alias in aliases:
        rec = records[alias]
        chk = check_one(
            alias=alias,
            record=rec,
            auto_install=auto_install,
            dry_run=dry_run,
        )

        item = {
            "package_name": chk.package_name,
            "installed": chk.installed,
            "installed_version": chk.installed_version,
            "version_spec": chk.version_spec,
            "satisfies": chk.satisfies,
            "note": chk.note,
        }

        if chk.installed and chk.satisfies is True:
            resolved[alias] = item
        elif chk.installed and chk.satisfies is False:
            unsatisfied[alias] = item
        else:
            missing[alias] = item

    all_satisfied = (len(missing) == 0) and (len(unsatisfied) == 0)

    result = {
        "status": "ok",
        "mode": "check",
        "all_satisfied": all_satisfied,
        "environment": env_snapshot(),
        "registry_dir": str(registry_dir),
        "auto_install": bool(auto_install),
        "dry_run": bool(dry_run),
        "resolved": resolved,
        "missing": missing,
        "unsatisfied": unsatisfied,
    }
    return result


# =====================================
# CLI / main 接口区
# =====================================
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Understanding-layer dependency resolver (preflight check)."
    )
    parser.add_argument(
        "--registry-dir",
        default=str(DEFAULT_REGISTRY_DIR),
        help="Registry directory. Default: config/understanding/dependencies",
    )
    parser.add_argument(
        "--depends",
        help="Comma separated alias list",
    )
    parser.add_argument(
        "--depends-file",
        help="JSON list or object containing depends list",
    )
    parser.add_argument(
        "--auto-install",
        action="store_true",
        help="Explicitly enable auto installation for missing registered packages",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run. No installation will be performed",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Return non-zero exit code when any missing or unsatisfied exists",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    registry_dir = Path(args.registry_dir)
    depends_file = Path(args.depends_file) if args.depends_file else None

    try:
        aliases = read_dep_aliases(args.depends, depends_file)
        if len(aliases) == 0:
            out = {
                "status": "ok",
                "mode": "check",
                "all_satisfied": True,
                "environment": env_snapshot(),
                "registry_dir": str(registry_dir),
                "auto_install": bool(args.auto_install),
                "dry_run": bool(args.dry_run),
                "resolved": {},
                "missing": {},
                "unsatisfied": {},
            }
            print(dumps_json(out))
            return 0

        result = run_check(
            registry_dir=registry_dir,
            aliases=aliases,
            auto_install=bool(args.auto_install),
            dry_run=bool(args.dry_run),
        )
        print(dumps_json(result))

        if args.fail_fast and not result.get("all_satisfied", False):
            return 2
        return 0

    except Exception as e:
        err = {
            "status": "error",
            "error_type": type(e).__name__,
            "error": str(e),
            "registry_dir": str(registry_dir),
        }
        print(dumps_json(err))
        return 1


# =====================================
# Entry Point
# =====================================
if __name__ == "__main__":
    raise SystemExit(main())
