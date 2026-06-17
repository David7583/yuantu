#!/usr/bin/env python3
# ============================================================
# 文件名: bulk_load_to_sql.py
# 定位: 高性能批量写入网关
# 说明: 绕过 SqlWriter 的逐行连接瓶颈，使用 executemany 实现秒级全量入库
# ============================================================

import argparse
import json
import sqlite3
import sys
from pathlib import Path

# 导入你的标准网关类
from sql_writer_v0001 import SqlWriter, _utc_now_iso

def load_jsonl(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

def main():
    p = argparse.ArgumentParser(description="High-performance Bulk Loader for Understand DB")
    p.add_argument("--config", required=True)
    p.add_argument("--concept", required=True)
    p.add_argument("--instance", required=True)
    p.add_argument("--attribute", required=True)
    p.add_argument("--run-id", required=True)
    args = p.parse_args()

    # 1. 初始化 Writer (只做一次 Schema 校验)
    writer = SqlWriter(args.config, strict=True, verify_schema_on_connect=True)
    cfg = writer.config
    run_id = args.run_id

    # 记录开始
    writer.record_run_event(run_id=run_id, script_name="bulk_load_to_sql", script_version="v0001", status="started", error_summary=None)

    # 2. 内存加载所有数据
    print(">>> 正在读取 JSONL 数据到内存...")
    concepts = load_jsonl(Path(args.concept))
    instances = load_jsonl(Path(args.instance))
    attributes = load_jsonl(Path(args.attribute))
    created_at = _utc_now_iso()

    # 3. 提取表名和字段映射
    t_c, f_c = cfg.tables["concept"], cfg.fields["concept"]
    t_i, f_i = cfg.tables["instance"], cfg.fields["instance"]
    t_a, f_a = cfg.tables["attribute"], cfg.fields["attribute"]

    # 4. 构建 Bulk 批量元组
    print(">>> 正在组装 SQL 载荷...")
    # Concept
    concept_tuples = [
        (r["concept_id"], r["unit_text"], r["content_hash"], None, created_at, r.get("schema_version", "v0001"), run_id)
        for r in concepts
    ]
    
    # Instance
    instance_tuples = [
        (r["instance_id"], r["concept_id"], r.get("observed_at", {}).get("asset_id", ""), 
         r.get("observed_at", {}).get("path", ""), r.get("observed_at", {}).get("value_index"), 
         r.get("observed_at", {}).get("segment_index", 0), r.get("observed_at", {}).get("sentence_index"), 
         r.get("observed_at", {}).get("char_start"), r.get("observed_at", {}).get("char_end"), 
         r.get("unit_text", ""), r.get("content_hash", ""), created_at, r.get("schema_version", "v0001"), run_id)
        for r in instances
    ]

    # Attribute (需要生成稳定的 attr_id)
    # 因为原脚本的 _stable_attr_id 是私有的，我们在这里使用简单的哈希替代，或者调用内部方法
    from sql_writer_v0001 import _stable_attr_id
    
    attr_tuples = []
    for r in attributes:
        created_by = r.get("provenance", {}).get("script_name", "bulk_loader")
        a_id = _stable_attr_id(
            object_type=r["object_type"], object_id=r["object_id"], attr_scope=None, 
            attr_key=r["attr_key"], attr_value=r.get("attr_value"), attr_type=None, 
            attr_state=r.get("attr_state", "active"), evidence_ref=None, created_by=created_by
        )
        attr_tuples.append((
            a_id, r["object_type"], r["object_id"], None, r["attr_key"], r.get("attr_value"), 
            None, r.get("attr_state", "active"), None, created_at, created_by, run_id
        ))

    # 5. 一次性开启连接，极速写入
    print(">>> 正在执行 SQLite Executemany 原子写入...")
    conn = writer.connect()
    try:
        conn.execute("BEGIN TRANSACTION;")
        
        # 写入 Concept
        sql_c = f"INSERT OR IGNORE INTO {t_c} ({f_c['id']}, {f_c['text']}, {f_c['hash']}, {f_c['first_seen_instance']}, {f_c['created_at']}, {f_c['schema_version']}, {f_c['run_id']}) VALUES (?,?,?,?,?,?,?)"
        conn.executemany(sql_c, concept_tuples)
        
        # 写入 Instance
        sql_i = f"INSERT OR IGNORE INTO {t_i} ({f_i['id']}, {f_i['concept_id']}, {f_i['asset_id']}, {f_i['path']}, {f_i['value_index']}, {f_i['segment_index']}, {f_i['sentence_index']}, {f_i['char_start']}, {f_i['char_end']}, {f_i['content']}, {f_i['content_hash']}, {f_i['created_at']}, {f_i['schema_version']}, {f_i['run_id']}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        conn.executemany(sql_i, instance_tuples)
        
        # 写入 Attribute
        sql_a = f"INSERT OR IGNORE INTO {t_a} ({f_a['id']}, {f_a['object_type']}, {f_a['object_id']}, {f_a['attr_scope']}, {f_a['attr_key']}, {f_a['attr_value']}, {f_a['attr_type']}, {f_a['attr_state']}, {f_a['evidence_ref']}, {f_a['created_at']}, {f_a['created_by']}, {f_a['run_id']}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
        conn.executemany(sql_a, attr_tuples)
        
        conn.commit()
        print(f"[SUCCESS] 成功写入! Concepts: {len(concept_tuples)}, Instances: {len(instance_tuples)}, Attributes: {len(attr_tuples)}")
        
        writer.record_run_event(run_id=run_id, script_name="bulk_load_to_sql", script_version="v0001", status="finished", error_summary=None)
        
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 写入失败，事务已回滚: {e}")
        writer.record_run_event(run_id=run_id, script_name="bulk_load_to_sql", script_version="v0001", status="finished", error_summary=str(e)[:200])
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()