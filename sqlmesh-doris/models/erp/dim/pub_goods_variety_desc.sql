MODEL (
  name sqlmesh_dim.erp_pub_goods_variety_desc,
  kind FULL,
  dialect doris,
  grain varietydescid,
  physical_properties (
    unique_key = (valid_from, valid_to, varietydescid),
    distributed_by = (kind='HASH', expressions=varietydescid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "商品品种描述维度表",
  column_descriptions (
    varietydescid = "品种描述ID",
    opcode = "操作码",
    varietydescno = "品种描述编号",
    varietydescname = "品种描述名称",
    vardesclassid = "品种描述分类ID",
    vardesclassopcode = "品种描述分类操作码",
    vardesclassno = "品种描述分类编号",
    vardesclassname = "品种描述分类名称"
  )
);

WITH ranked_data AS (
    SELECT VARIETYDESCID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY VARIETYDESCID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    VARIETYDESCID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_goods_variety_desc
)
SELECT 
CASE
  WHEN a.record_seq = 1 
  THEN DATE('1970-01-01')
  ELSE date(b.DW_CREATETIME)
END AS valid_from,
CASE
  WHEN a.next_start_time IS NOT NULL THEN a.next_start_time
  ELSE DATE('9999-12-31')
END AS valid_to,
  b.varietydescid,
  b.opcode,
  b.varietydescno,
  b.varietydescname,
  b.vardesclassid,
  c.opcode AS vardesclassopcode,
  c.vardesclassno,
  c.vardesclassname
FROM ranked_data a
JOIN ods_erp.pub_goods_variety_desc b ON a.varietydescid=b.varietydescid AND a.DW_CREATETIME=b.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_goods_variety_desc_class c ON b.vardesclassid = c.vardesclassid AND c.valid_to = DATE('9999-12-31') 