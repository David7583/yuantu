#!/usr/bin/env python3
"""
测试辅助脚本：批量调用 sql_writer 写入三类声明
不是架构组件，仅用于测试链路是否跑通。
"""

import json
import sys
from pathlib import Path

# 直接 import SqlWriter 类，避免 410 次进程启动
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sql_writer_v0001 import SqlWriter


def load_jsonl(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main():
    import argparse
    p = argparse.ArgumentParser(description="Batch write declarations to understand.db via SqlWriter")
    p.add_argument("--config", required=True, help="sql_writer_config_v0001.yml path")
    p.add_argument("--concept", required=True, help="concept_declarations.jsonl")
    p.add_argument("--instance", required=True, help="instance_declarations.jsonl")
    p.add_argument("--attribute", required=True, help="attribute_declarations.jsonl")
    p.add_argument("--run-id", required=True, help="run_id for this batch")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    writer = SqlWriter(args.config, strict=True, verify_schema_on_connect=True)
    dry = bool(args.dry_run)
    run_id = args.run_id

    # record run start
    writer.record_run_event(
        run_id=run_id,
        script_name="batch_write_test",
        script_version="v0001",
        status="started",
        dry_run=dry,
        error_summary=None,  # 修复：补充了缺失的 error_summary 参数
    )

    stats = {"concept": 0, "instance": 0, "attribute": 0, "errors": []}

    # 1. concepts
    concepts = load_jsonl(Path(args.concept))
    for rec in concepts:
        try:
            writer.write_concept(
                unit_text_id=rec["concept_id"],
                unit_text=rec["unit_text"],
                content_hash=rec["content_hash"],
                schema_version=rec.get("schema_version", "v0001"),
                first_seen_instance_id=None,
                run_id=run_id,
                dry_run=dry,
            )
            stats["concept"] += 1
        except Exception as e:
            stats["errors"].append({"type": "concept", "error": str(e)[:200]})

    # 2. instances
    instances = load_jsonl(Path(args.instance))
    for rec in instances:
        obs = rec.get("observed_at", {})
        try:
            writer.write_instance(
                instance_id=rec["instance_id"],
                unit_text_id=rec["concept_id"],
                asset_id=obs.get("asset_id", ""),
                path=obs.get("path", ""),
                value_index=obs.get("value_index"),
                segment_index=obs.get("segment_index", 0),
                sentence_index=obs.get("sentence_index"),
                char_start=obs.get("char_start"),
                char_end=obs.get("char_end"),
                content=rec.get("unit_text", ""),
                content_hash=rec.get("content_hash", ""),
                schema_version=rec.get("schema_version", "v0001"),
                run_id=run_id,
                dry_run=dry,
            )
            stats["instance"] += 1
        except Exception as e:
            stats["errors"].append({"type": "instance", "id": rec.get("instance_id", "?")[:16], "error": str(e)[:200]})

    # 3. attributes
    attributes = load_jsonl(Path(args.attribute))
    for rec in attributes:
        try:
            writer.write_attribute(
                object_type=rec["object_type"],
                object_id=rec["object_id"],
                attr_key=rec["attr_key"],
                attr_value=rec.get("attr_value"),
                attr_type=None,
                attr_scope=None,
                attr_state=rec.get("attr_state", "active"),
                evidence_ref=None,
                created_by=rec.get("provenance", {}).get("script_name", "ingest_attribute_units"),
                run_id=run_id,
                dry_run=dry,
            )
            stats["attribute"] += 1
        except Exception as e:
            stats["errors"].append({"type": "attribute", "error": str(e)[:200]})

    # record run finish
    writer.record_run_event(
        run_id=run_id,
        script_name="batch_write_test",
        script_version="v0001",
        status="finished",
        dry_run=dry,
        error_summary=f"errors={len(stats['errors'])}" if stats["errors"] else None, # 修复：补充了缺失的 error_summary 参数并汇总错误数
    )

    result = {
        "status": "ok" if not stats["errors"] else "partial",
        "dry_run": dry,
        "run_id": run_id,
        "written": {
            "concepts": stats["concept"],
            "instances": stats["instance"],
            "attributes": stats["attribute"],
        },
        "error_count": len(stats["errors"]),
        "errors_sample": stats["errors"][:5],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not stats["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())