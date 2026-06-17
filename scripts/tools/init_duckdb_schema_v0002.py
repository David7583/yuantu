#!/usr/bin/env python3
# ============================================================
# File: init_duckdb_schema_v0002.py
# 中文名: DuckDB 派生层理解端数据库初始化与结构校验脚本
# Version: v0002
# Layer: infrastructure
# Main Layer: understand
# Script Type: Schema Management
# Updatable: True
#
# Purpose
#
# 创建 DuckDB 数据库文件与表结构（派生层理解端分析子系统）
# 或校验已有数据库的表结构与预期是否一致
#
# 本脚本遵循派生层手册 v0003 的设计原则
# 并与 duckdb_schema_config YAML 的字段映射保持对齐
#
#
# What it does
#
# 1 读取 duckdb_schema_config YAML 获取数据库路径与表名/字段名映射
# 2 创建目录结构（derivation/understand/duckdb/, derivation/understand/embedding/）
# 3 创建数据库文件与四张表（_sync_log / instance_units_sync / concept_units_sync / attribute_units_sync）
# 4 创建三个视图（instance_units_latest / concept_units_latest / attribute_units_latest）
# 5 校验已有数据库的表结构、字段、视图完整性
# 6 报告已有表的行数（--validate 模式下）
#
#
# What it does NOT do
#
# 1 不写入任何业务数据
# 2 不执行同步操作
# 3 不做分析计算
# 4 不修改 config 文件
# 5 不创建 action 端的数据库（那是另一个脚本的职责）
# 6 不删除或重建已有表
#
#
# v0002 变更说明
#
# - 新增 attribute_units_sync 表（对齐 SQL库层 unit_attributes 表）
# - 新增 attribute_units_latest 视图
# - init 时连接使用 try/finally 确保连接必定关闭
# - validate 时报告各表行数（辅助判断是否已有数据）
# - validate 时使用 duckdb_views() 系统表检查视图（比 SELECT LIMIT 0 更可靠）
# - _load_config 对 YAML 解析失败做防御处理而非静默降级
# - 对已有 v0001 库完全兼容：IF NOT EXISTS 跳过已有表，OR REPLACE 更新视图
#
#
# Design decision
#
# 幂等性保证
#
# 所有 CREATE TABLE 使用 IF NOT EXISTS
# 所有 CREATE VIEW 使用 CREATE OR REPLACE
# 重复运行不会破坏已有数据
#
# Config 驱动
#
# 表名与字段名从 duckdb_schema_config YAML 中读取
# 确保建库脚本与同步脚本使用完全相同的名称
# 若 config 文件不存在 则使用内置默认值
#
# 路径解析
#
# 始终以 _find_project_root 找到的 project_root 为基准
# config 中的 duckdb path 作为相对 project_root 的路径解释
#
# 字段名安全
#
# 所有从 config 读取的表名与字段名
# 必须通过合法性检查（仅允许字母、数字、下划线）
# 防止配置文件被污染时执行任意 SQL
#
# ============================================================


# ============================================================
# ALIAS_META
# ============================================================

# alias: init_duckdb_schema
# family: init_duckdb_schema
# role: schema_initializer
# version: v0002
# status: active
# entry_point: scripts/tools/init_duckdb_schema_v0002.py
#
# depends_on:
#   - Python stdlib: json, argparse, pathlib, datetime, typing, re, sys, traceback
#   - Third-party: duckdb, PyYAML (yaml)
#
# used_by:
#   - manual invocation before first sync
#   - sync_sql_to_duckdb (expects tables to exist)
# ============================================================


from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# Constants
# ============================================================

SCRIPT_NAME = "init_duckdb_schema_v0002.py"
SCRIPT_VERSION = "v0002"

MAX_ROOT_SEARCH_DEPTH = 10

# Default database path (relative to project root)
DEFAULT_DB_PATH = Path("derivation") / "understand" / "duckdb" / "understand_analysis.duckdb"

# Default table names
DEFAULT_TABLES = {
    "sync_log": "_sync_log",
    "instance_sync": "instance_units_sync",
    "concept_sync": "concept_units_sync",
    "attribute_sync": "attribute_units_sync",
}

# Default view names
DEFAULT_VIEWS = {
    "instance_latest": "instance_units_latest",
    "concept_latest": "concept_units_latest",
    "attribute_latest": "attribute_units_latest",
}

# Default field names
DEFAULT_FIELDS = {
    "sync_log": {
        "sync_id": "sync_id",
        "source_db": "source_db",
        "source_table": "source_table",
        "record_count": "record_count",
        "started_at": "started_at",
        "finished_at": "finished_at",
        "script_name": "script_name",
        "script_version": "script_version",
        "source_db_hash": "source_db_hash",
    },
    "instance_sync": {
        "sync_id": "sync_id",
        "synced_at": "synced_at",
        "instance_id": "instance_id",
        "unit_text_id": "unit_text_id",
        "asset_id": "asset_id",
        "path": "path",
        "value_index": "value_index",
        "segment_index": "segment_index",
        "sentence_index": "sentence_index",
        "char_start": "char_start",
        "char_end": "char_end",
        "content": "content",
        "content_hash": "content_hash",
        "created_at": "created_at",
        "schema_version": "schema_version",
        "run_id": "run_id",
    },
    "concept_sync": {
        "sync_id": "sync_id",
        "synced_at": "synced_at",
        "unit_text_id": "unit_text_id",
        "unit_text": "unit_text",
        "content_hash": "content_hash",
        "first_seen_instance_id": "first_seen_instance_id",
        "created_at": "created_at",
        "schema_version": "schema_version",
        "run_id": "run_id",
    },
    "attribute_sync": {
        "sync_id": "sync_id",
        "synced_at": "synced_at",
        "attr_id": "attr_id",
        "object_type": "object_type",
        "object_id": "object_id",
        "attr_scope": "attr_scope",
        "attr_key": "attr_key",
        "attr_value": "attr_value",
        "attr_type": "attr_type",
        "attr_state": "attr_state",
        "evidence_ref": "evidence_ref",
        "created_at": "created_at",
        "created_by": "created_by",
        "run_id": "run_id",
    },
}

# Directory structure to create
DEFAULT_DIRECTORIES = [
    Path("derivation") / "understand" / "duckdb",
    Path("derivation") / "understand" / "embedding",
    Path("derivation") / "action" / "duckdb",
    Path("derivation") / "action" / "embedding",
]

# Regex for validating SQL identifiers from config
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ============================================================
# Utilities
# ============================================================

def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _find_project_root(start: Path, max_up: int = MAX_ROOT_SEARCH_DEPTH) -> Path:
    cur = start.resolve()
    for _ in range(max_up):
        if (cur / "scripts").is_dir() and (cur / "config").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return Path.cwd().resolve()


def _validate_identifier(name: str, context: str) -> None:
    """Ensure a table or field name contains only safe characters."""
    if not isinstance(name, str) or not name:
        raise ValueError(
            f"Empty or non-string SQL identifier from config ({context})."
        )
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"Unsafe SQL identifier from config ({context}): '{name}'. "
            f"Only letters, digits, and underscores are allowed."
        )


def _validate_all_identifiers(
    tables: Dict[str, str],
    views: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> None:
    """Validate all table, view, and field names from config."""
    for key, tbl_name in tables.items():
        _validate_identifier(tbl_name, f"tables.{key}")
    for key, view_name in views.items():
        _validate_identifier(view_name, f"views.{key}")
    for group_key, field_map in fields.items():
        if not isinstance(field_map, dict):
            raise ValueError(f"fields.{group_key} must be a mapping, got {type(field_map).__name__}")
        for fkey, fname in field_map.items():
            _validate_identifier(fname, f"fields.{group_key}.{fkey}")


def _check_required_field_groups(fields: Dict[str, Dict[str, str]]) -> List[str]:
    """Check that all required field groups are present and non-empty."""
    issues = []
    required_groups = ["sync_log", "instance_sync", "concept_sync", "attribute_sync"]
    for group in required_groups:
        if group not in fields:
            issues.append(f"missing required field group: {group}")
        elif not fields[group]:
            issues.append(f"empty field group: {group}")
    return issues


def _load_config(
    config_path: Path,
    project_root: Path,
    verbose: bool = False,
) -> Tuple[Path, Dict[str, str], Dict[str, str], Dict[str, Dict[str, str]], List[str]]:
    """Load duckdb_schema config YAML.

    Returns (db_path, tables, views, fields, warnings).
    Falls back to defaults on missing config. Raises on malformed config.
    """
    warnings: List[str] = []

    try:
        import yaml
    except ImportError:
        warnings.append("PyYAML not installed, using built-in defaults for all config values.")
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES.copy(), DEFAULT_VIEWS.copy(), _deep_copy_fields(DEFAULT_FIELDS), warnings

    if not config_path.exists():
        warnings.append(f"Config file not found: {config_path}. Using built-in defaults.")
        db_path = (project_root / DEFAULT_DB_PATH).resolve()
        return db_path, DEFAULT_TABLES.copy(), DEFAULT_VIEWS.copy(), _deep_copy_fields(DEFAULT_FIELDS), warnings

    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"Failed to parse config YAML at {config_path}: {e}") from e

    if not isinstance(raw, dict):
        raise ValueError(f"Config file must be a YAML mapping (dict), got {type(raw).__name__} at {config_path}")

    if verbose:
        print(f"[CONFIG] Loaded: {config_path}")

    # --- connection ---
    conn = raw.get("connection", {}) or {}
    duckdb_cfg = conn.get("duckdb", {}) or {}
    db_path_str = duckdb_cfg.get("path", str(DEFAULT_DB_PATH))

    # --- tables (merge with defaults to ensure new keys are present) ---
    tables = DEFAULT_TABLES.copy()
    raw_tables = raw.get("tables")
    if isinstance(raw_tables, dict):
        tables.update(raw_tables)
    elif raw_tables is not None:
        warnings.append(f"Config 'tables' is not a mapping ({type(raw_tables).__name__}), using defaults.")

    # --- views (merge with defaults) ---
    views = DEFAULT_VIEWS.copy()
    views_cfg = raw.get("views")
    if isinstance(views_cfg, dict):
        for key, view_def in views_cfg.items():
            if isinstance(view_def, dict) and "name" in view_def:
                views[key] = view_def["name"]
            elif isinstance(view_def, str):
                views[key] = view_def
            else:
                warnings.append(f"Config views.{key} has unexpected format, using default.")
    elif views_cfg is not None:
        warnings.append(f"Config 'views' is not a mapping ({type(views_cfg).__name__}), using defaults.")

    # --- fields (merge with defaults) ---
    fields = _deep_copy_fields(DEFAULT_FIELDS)
    raw_fields = raw.get("fields")
    if isinstance(raw_fields, dict):
        for group_key, group_val in raw_fields.items():
            if isinstance(group_val, dict):
                if group_key not in fields:
                    fields[group_key] = {}
                fields[group_key].update(group_val)
            else:
                warnings.append(f"Config fields.{group_key} is not a mapping, skipping.")
    elif raw_fields is not None:
        warnings.append(f"Config 'fields' is not a mapping ({type(raw_fields).__name__}), using defaults.")

    # Always resolve relative to project_root
    db_path = (project_root / db_path_str).resolve()

    return db_path, tables, views, fields, warnings


def _deep_copy_fields(fields: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """Deep copy the fields dict to prevent mutation of defaults."""
    return {k: dict(v) for k, v in fields.items()}


def _ensure_directories(project_root: Path, verbose: bool = False) -> List[str]:
    """Create the derivation directory structure."""
    created = []
    for rel_dir in DEFAULT_DIRECTORIES:
        abs_dir = project_root / rel_dir
        if not abs_dir.exists():
            abs_dir.mkdir(parents=True, exist_ok=True)
            created.append(str(rel_dir))
            if verbose:
                print(f"[DIR] Created: {rel_dir}")

        # Add .gitkeep to empty directories
        gitkeep = abs_dir / ".gitkeep"
        if not gitkeep.exists():
            try:
                if not any(abs_dir.iterdir()):
                    gitkeep.write_text("# Placeholder for empty directory\n", encoding="utf-8")
            except OSError:
                pass

    return created


# ============================================================
# Schema Definition
# ============================================================

def _build_create_statements(
    tables: Dict[str, str],
    views: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
) -> List[Tuple[str, str]]:
    """Build CREATE TABLE and CREATE VIEW statements from config mappings.

    Returns list of (label, sql) tuples for diagnostics.
    """

    stmts: List[Tuple[str, str]] = []

    # --- table aliases ---
    t_sync_log = tables["sync_log"]
    t_instance = tables["instance_sync"]
    t_concept = tables["concept_sync"]
    t_attribute = tables["attribute_sync"]

    f_sync = fields["sync_log"]
    f_inst = fields["instance_sync"]
    f_conc = fields["concept_sync"]
    f_attr = fields["attribute_sync"]

    v_inst_latest = views["instance_latest"]
    v_conc_latest = views["concept_latest"]
    v_attr_latest = views["attribute_latest"]

    # -------------------------------------------------------
    # 1. _sync_log
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {t_sync_log}", f"""
CREATE TABLE IF NOT EXISTS {t_sync_log} (
    {f_sync['sync_id']}           VARCHAR PRIMARY KEY,
    {f_sync['source_db']}         VARCHAR NOT NULL,
    {f_sync['source_table']}      VARCHAR NOT NULL,
    {f_sync['record_count']}      INTEGER NOT NULL,
    {f_sync['started_at']}        TIMESTAMP NOT NULL,
    {f_sync['finished_at']}       TIMESTAMP,
    {f_sync['script_name']}       VARCHAR NOT NULL,
    {f_sync['script_version']}    VARCHAR NOT NULL,
    {f_sync['source_db_hash']}    VARCHAR
);
"""))

    # -------------------------------------------------------
    # 2. instance_units_sync
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {t_instance}", f"""
CREATE TABLE IF NOT EXISTS {t_instance} (
    {f_inst['sync_id']}           VARCHAR NOT NULL,
    {f_inst['synced_at']}         TIMESTAMP NOT NULL,
    {f_inst['instance_id']}       VARCHAR NOT NULL,
    {f_inst['unit_text_id']}      VARCHAR NOT NULL,
    {f_inst['asset_id']}          VARCHAR NOT NULL,
    {f_inst['path']}              VARCHAR NOT NULL,
    {f_inst['value_index']}       INTEGER,
    {f_inst['segment_index']}     INTEGER NOT NULL,
    {f_inst['sentence_index']}    INTEGER,
    {f_inst['char_start']}        INTEGER,
    {f_inst['char_end']}          INTEGER,
    {f_inst['content']}           VARCHAR NOT NULL,
    {f_inst['content_hash']}      VARCHAR NOT NULL,
    {f_inst['created_at']}        VARCHAR NOT NULL,
    {f_inst['schema_version']}    VARCHAR NOT NULL,
    {f_inst['run_id']}            VARCHAR NOT NULL,
    PRIMARY KEY ({f_inst['sync_id']}, {f_inst['instance_id']})
);
"""))

    # -------------------------------------------------------
    # 3. concept_units_sync
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {t_concept}", f"""
CREATE TABLE IF NOT EXISTS {t_concept} (
    {f_conc['sync_id']}                 VARCHAR NOT NULL,
    {f_conc['synced_at']}               TIMESTAMP NOT NULL,
    {f_conc['unit_text_id']}            VARCHAR NOT NULL,
    {f_conc['unit_text']}               VARCHAR NOT NULL,
    {f_conc['content_hash']}            VARCHAR NOT NULL,
    {f_conc['first_seen_instance_id']}  VARCHAR,
    {f_conc['created_at']}              VARCHAR NOT NULL,
    {f_conc['schema_version']}          VARCHAR NOT NULL,
    {f_conc['run_id']}                  VARCHAR NOT NULL,
    PRIMARY KEY ({f_conc['sync_id']}, {f_conc['unit_text_id']})
);
"""))

    # -------------------------------------------------------
    # 4. attribute_units_sync  [v0002 新增]
    # -------------------------------------------------------
    stmts.append((f"CREATE TABLE {t_attribute}", f"""
CREATE TABLE IF NOT EXISTS {t_attribute} (
    {f_attr['sync_id']}           VARCHAR NOT NULL,
    {f_attr['synced_at']}         TIMESTAMP NOT NULL,
    {f_attr['attr_id']}           VARCHAR NOT NULL,
    {f_attr['object_type']}       VARCHAR NOT NULL,
    {f_attr['object_id']}         VARCHAR NOT NULL,
    {f_attr['attr_scope']}        VARCHAR,
    {f_attr['attr_key']}          VARCHAR NOT NULL,
    {f_attr['attr_value']}        VARCHAR,
    {f_attr['attr_type']}         VARCHAR,
    {f_attr['attr_state']}        VARCHAR NOT NULL,
    {f_attr['evidence_ref']}      VARCHAR,
    {f_attr['created_at']}        VARCHAR NOT NULL,
    {f_attr['created_by']}        VARCHAR NOT NULL,
    {f_attr['run_id']}            VARCHAR NOT NULL,
    PRIMARY KEY ({f_attr['sync_id']}, {f_attr['attr_id']})
);
"""))

    # -------------------------------------------------------
    # 5. instance_units_latest view
    # -------------------------------------------------------
    stmts.append((f"CREATE VIEW {v_inst_latest}", f"""
CREATE OR REPLACE VIEW {v_inst_latest} AS
SELECT * FROM {t_instance}
WHERE {f_inst['sync_id']} = (
    SELECT {f_sync['sync_id']}
    FROM {t_sync_log}
    WHERE {f_sync['source_table']} = 'instance_units'
    ORDER BY {f_sync['finished_at']} DESC
    LIMIT 1
);
"""))

    # -------------------------------------------------------
    # 6. concept_units_latest view
    # -------------------------------------------------------
    stmts.append((f"CREATE VIEW {v_conc_latest}", f"""
CREATE OR REPLACE VIEW {v_conc_latest} AS
SELECT * FROM {t_concept}
WHERE {f_conc['sync_id']} = (
    SELECT {f_sync['sync_id']}
    FROM {t_sync_log}
    WHERE {f_sync['source_table']} = 'concept_units'
    ORDER BY {f_sync['finished_at']} DESC
    LIMIT 1
);
"""))

    # -------------------------------------------------------
    # 7. attribute_units_latest view  [v0002 新增]
    # -------------------------------------------------------
    stmts.append((f"CREATE VIEW {v_attr_latest}", f"""
CREATE OR REPLACE VIEW {v_attr_latest} AS
SELECT * FROM {t_attribute}
WHERE {f_attr['sync_id']} = (
    SELECT {f_sync['sync_id']}
    FROM {t_sync_log}
    WHERE {f_sync['source_table']} = 'unit_attributes'
    ORDER BY {f_sync['finished_at']} DESC
    LIMIT 1
);
"""))

    return stmts


# ============================================================
# Init
# ============================================================

def init_database(
    db_path: Path,
    tables: Dict[str, str],
    views: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
    project_root: Path,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Initialize DuckDB database with schema. Idempotent and safe on existing databases."""

    try:
        import duckdb
    except ImportError:
        return {
            "status": "error",
            "action": "init",
            "error": "duckdb module not installed. Run: pip install duckdb",
            "timestamp": _utc_iso(),
        }

    current_label = "setup"
    conn = None

    try:
        # Validate identifiers before touching anything
        _validate_all_identifiers(tables, views, fields)

        # Check required field groups
        group_issues = _check_required_field_groups(fields)
        if group_issues:
            return {
                "status": "error",
                "action": "init",
                "error": f"Config validation failed: {'; '.join(group_issues)}",
                "timestamp": _utc_iso(),
            }

        # Create directory structure
        dirs_created = _ensure_directories(project_root, verbose=verbose)

        # Ensure database directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Detect if database already exists (for reporting)
        db_existed = db_path.exists()

        # Build SQL statements
        statements = _build_create_statements(tables, views, fields)

        # Connect and execute
        current_label = "connect"
        conn = duckdb.connect(str(db_path))

        executed = []
        failed: List[Dict[str, str]] = []

        for label, sql in statements:
            current_label = label
            if verbose:
                print(f"[SQL] {label}")
            try:
                conn.execute(sql)
                executed.append(label)
            except Exception as e:
                error_msg = str(e)[:300]
                # IF NOT EXISTS / OR REPLACE should prevent this, but log it rather than crash
                failed.append({"label": label, "error": error_msg})
                if verbose:
                    print(f"[WARN] {label} failed: {error_msg}")

        # Post-init: count rows in each table for sanity check
        row_counts = {}
        for config_key, table_name in tables.items():
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                row_counts[table_name] = result[0] if result else 0
            except Exception:
                row_counts[table_name] = -1  # table might not exist if creation failed

        conn.close()
        conn = None

        status = "ok" if not failed else "partial"

        return {
            "status": status,
            "action": "init",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "db_path": str(db_path),
            "db_existed_before": db_existed,
            "tables_expected": list(tables.values()),
            "views_expected": list(views.values()),
            "statements_executed": executed,
            "statements_failed": failed,
            "row_counts": row_counts,
            "directories_created": dirs_created,
            "timestamp": _utc_iso(),
        }

    except Exception as e:
        return {
            "status": "error",
            "action": "init",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "db_path": str(db_path),
            "failed_at": current_label,
            "error": str(e)[:500],
            "traceback": traceback.format_exc()[-800:],
            "timestamp": _utc_iso(),
        }

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# Validate
# ============================================================

def validate_schema(
    db_path: Path,
    tables: Dict[str, str],
    views: Dict[str, str],
    fields: Dict[str, Dict[str, str]],
    verbose: bool = False,
) -> Dict[str, Any]:
    """Validate that existing database matches expected schema.

    Reports missing tables, missing columns, missing views, and row counts.
    """

    try:
        import duckdb
    except ImportError:
        return {
            "status": "error",
            "action": "validate",
            "error": "duckdb module not installed. Run: pip install duckdb",
            "timestamp": _utc_iso(),
        }

    if not db_path.exists():
        return {
            "status": "error",
            "action": "validate",
            "db_path": str(db_path),
            "error": "database file does not exist",
            "hint": "run with --init first",
            "timestamp": _utc_iso(),
        }

    try:
        _validate_all_identifiers(tables, views, fields)
    except ValueError as e:
        return {
            "status": "error",
            "action": "validate",
            "error": f"Identifier validation failed: {e}",
            "timestamp": _utc_iso(),
        }

    issues: List[str] = []
    row_counts: Dict[str, int] = {}
    conn = None

    try:
        conn = duckdb.connect(str(db_path), read_only=True)

        # Get list of tables
        result = conn.execute("SHOW TABLES").fetchall()
        actual_tables = {row[0] for row in result}

        if verbose:
            print(f"[VALIDATE] Tables in database: {sorted(actual_tables)}")

        # Check each expected table exists and has expected columns
        for config_key, table_name in tables.items():
            if table_name not in actual_tables:
                issues.append(f"missing table: {table_name}")
                row_counts[table_name] = -1
                continue

            # Get actual columns
            try:
                col_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
                actual_cols = {row[0] for row in col_result}
            except Exception as e:
                issues.append(f"table {table_name}: DESCRIBE failed: {str(e)[:100]}")
                row_counts[table_name] = -1
                continue

            # Check expected columns exist
            if config_key in fields:
                for fkey, fname in fields[config_key].items():
                    if fname not in actual_cols:
                        issues.append(f"table {table_name}: missing column: {fname}")

                # Report unexpected columns (informational, not an issue)
                expected_cols = set(fields[config_key].values())
                extra_cols = actual_cols - expected_cols
                if extra_cols and verbose:
                    print(f"[INFO] table {table_name}: extra columns (not in config): {sorted(extra_cols)}")

            # Row count
            try:
                cnt = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                row_counts[table_name] = cnt[0] if cnt else 0
            except Exception:
                row_counts[table_name] = -1

        # Check views using duckdb_views() system table
        try:
            view_result = conn.execute(
                "SELECT view_name FROM duckdb_views() WHERE NOT internal"
            ).fetchall()
            actual_views = {row[0] for row in view_result}
        except Exception:
            # Fallback: try SELECT LIMIT 0 on each view
            actual_views = set()
            for view_key, view_name in views.items():
                try:
                    conn.execute(f"SELECT * FROM {view_name} LIMIT 0")
                    actual_views.add(view_name)
                except Exception:
                    pass

        for view_key, view_name in views.items():
            if view_name not in actual_views:
                issues.append(f"missing or invalid view: {view_name}")
            elif verbose:
                # Report view row count
                try:
                    cnt = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
                    print(f"[VIEW] {view_name}: {cnt[0] if cnt else 0} rows")
                except Exception:
                    print(f"[VIEW] {view_name}: query failed (view might reference empty sync_log)")

        conn.close()
        conn = None

        status = "ok" if not issues else "issues_found"

        return {
            "status": status,
            "action": "validate",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "db_path": str(db_path),
            "tables_checked": list(tables.values()),
            "views_checked": list(views.values()),
            "row_counts": row_counts,
            "issues": issues,
            "issues_count": len(issues),
            "timestamp": _utc_iso(),
        }

    except Exception as e:
        return {
            "status": "error",
            "action": "validate",
            "script": SCRIPT_NAME,
            "script_version": SCRIPT_VERSION,
            "db_path": str(db_path),
            "error": str(e)[:500],
            "traceback": traceback.format_exc()[-800:],
            "timestamp": _utc_iso(),
        }

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Initialize or validate DuckDB schema (派生层理解端分析子系统). v0002: +attribute table."
    )
    ap.add_argument(
        "--config",
        default=None,
        help="Path to duckdb_schema_config YAML. If omitted, searches project config/ or uses built-in defaults.",
    )
    ap.add_argument(
        "--db-path",
        default=None,
        help="Override database file path. If omitted, uses config or default.",
    )

    group = ap.add_mutually_exclusive_group()
    group.add_argument(
        "--init",
        action="store_true",
        help="Create database and tables (idempotent, safe on existing databases).",
    )
    group.add_argument(
        "--validate",
        action="store_true",
        help="Validate existing database schema and report row counts.",
    )

    ap.add_argument("--verbose", action="store_true", help="Print detailed progress to stderr/stdout.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    # Resolve project root
    here = Path(__file__).resolve()
    project_root = _find_project_root(here)

    # Load config (always relative to project_root)
    if args.config:
        config_path = Path(args.config).resolve()
    else:
        # Try v0002 config first, fall back to v0001 for backward compat
        config_path_v2 = project_root / "config" / "duckdb_schema_config_v0002.yml"
        config_path_v1 = project_root / "config" / "duckdb_schema_config_v0001.yml"
        if config_path_v2.exists():
            config_path = config_path_v2
        elif config_path_v1.exists():
            config_path = config_path_v1
        else:
            # Neither config exists — _load_config will use built-in defaults
            # Use v0002 path as the "expected" path for clearer diagnostics
            config_path = config_path_v2

    try:
        db_path, tables, views, fields, config_warnings = _load_config(
            config_path, project_root, verbose=bool(args.verbose)
        )
    except (ValueError, OSError) as e:
        print(json.dumps({
            "status": "error",
            "action": "load_config",
            "error": str(e)[:500],
            "config_path": str(config_path),
            "timestamp": _utc_iso(),
        }, ensure_ascii=False, indent=2))
        return 1

    # Override db_path if specified via CLI
    if args.db_path:
        db_path = Path(args.db_path).resolve()

    # Default action: validate
    if not args.init and not args.validate:
        args.validate = True

    if args.init:
        result = init_database(
            db_path, tables, views, fields, project_root,
            verbose=bool(args.verbose),
        )
    else:
        result = validate_schema(
            db_path, tables, views, fields,
            verbose=bool(args.verbose),
        )

    # Attach config warnings to result
    if config_warnings:
        result["config_warnings"] = config_warnings

    print(json.dumps(result, ensure_ascii=False, indent=2))

    if result["status"] == "error":
        return 1
    if result.get("issues"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
