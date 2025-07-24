MODEL (
  name sqlmesh_dim.erp_pub_goods_variety,
  kind FULL,
  dialect doris,
  grain varietyid,
  physical_properties (
    unique_key = (valid_from, valid_to, varietyid),
    distributed_by = (kind='HASH', expressions=varietyid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "商品大中小类品种描述维度表",
);

WITH ranked_data AS (
    SELECT varietyid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY varietyid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    varietyid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_goods_variety
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
b.varietyid,
b.opcode,
b.varietyno,
b.varietyname,
b.usestatus,
b.indicationid,
b.priceinterval,
b.varietydescid,
c.varietydescno,
CASE 
    WHEN b.varietyname LIKE '%家居环境清洁类%' and c.varietydescname is NULL THEN '家居环境清洁类'
    WHEN b.varietyname LIKE '%冲任饮品%' and c.varietydescname is NULL THEN '冲任饮品'
    ELSE c.varietydescname
END AS varietydescname,
c.vardesclassid,
c.vardesclassno,
CASE
    WHEN b.varietyname LIKE '%家居环境清洁类%' and c.vardesclassname is NULL THEN '生活用品'
    WHEN b.varietyname LIKE '%冲任饮品%' and c.vardesclassname is NULL THEN '保健食品'
    ELSE c.vardesclassname
END AS vardesclassname
FROM ranked_data a
JOIN ods_erp.pub_goods_variety b ON a.varietyid=b.varietyid AND a.DW_CREATETIME=b.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_goods_variety_desc c ON b.varietydescid = c.varietydescid AND c.valid_to = DATE('9999-12-31') 