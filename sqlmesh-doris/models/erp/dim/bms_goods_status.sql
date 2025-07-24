MODEL (
  name sqlmesh_dim.erp_bms_goods_status,
  kind FULL,
  dialect doris,
  grain (goodsstatusid),
  physical_properties (
    unique_key = (valid_from, valid_to, goodsstatusid),
    distributed_by = (kind='HASH', expressions=goodsstatusid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "货品状态维度表",
  column_descriptions (
    goodsstatusid = "货品状态ID",
    goodsstatus = "状态名称",
    useflag = "货品可销状态（0不可销、1可销）",
    qualitystatus = "质量状态（0不合格、1合格、2未确定、3待验）",
    priority = "优先级（0优先、1正常、2滞后）"
  )
);

WITH ranked_data AS (
    SELECT goodsstatusid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY goodsstatusid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    goodsstatusid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.bms_goods_status
)
SELECT
  CASE
    WHEN r.record_seq = 1 
    THEN DATE('1970-01-01')
    ELSE date(a.DW_CREATETIME)
  END AS valid_from,
  CASE
    WHEN r.next_start_time IS NOT NULL THEN r.next_start_time
    ELSE DATE('9999-12-31')
  END AS valid_to,
  a.goodsstatusid,
  a.goodsstatus,
  a.useflag,
  a.qualitystatus,
  a.priority
FROM ranked_data r
JOIN ods_erp.bms_goods_status a ON r.goodsstatusid = a.goodsstatusid AND r.DW_CREATETIME = a.DW_CREATETIME; 