MODEL (
  name sqlmesh_dim.erp_bms_busiscope_def,
  kind FULL,
  dialect doris,
  grain (keyid AS erp_bms_busiscope_def_id),
  physical_properties (
    unique_key = (valid_from, valid_to, keyid),
    distributed_by = (kind='HASH', expressions=keyid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "业务范围定义维度表",
  column_descriptions (
    keyid = "主键ID",
    scopedefid = "范围定义ID",
    scopename = "范围名称",
    category = "类别",
    groupmanagetype = "集团管理类型",
    zx_finance_class = "财务分类",
    lbfl = "类别分类",
    zx_finance_class_name = "财务分类名称",
    lbflname = "类别分类名称",
    splx = "商品类型",
    jxyplx = "经营药品类型",
    scopetype = "范围类型",
    scopetypeid = "范围类型ID"
  )
);

WITH ranked_data AS (
    SELECT KEYID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY KEYID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    KEYID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.bms_busiscope_def
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
  b.KEYID,
  b.SCOPEDEFID,
  b.SCOPENAME,
  b.CATEGORY,
  b.GROUPMANAGETYPE,
  b.ZX_FINANCE_CLASS,
  b.LBFL,
  fc.ddlname AS zx_finance_class_name,
  lb.ddlname AS lbflname,
  b.splx,
  b.jxyplx,
  bt.scopetype,
  b.scopetypeid
FROM ranked_data a
JOIN ods_erp.bms_busiscope_def b ON a.KEYID=b.KEYID AND a.DW_CREATETIME=b.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_ddl_dtl fc ON b.ZX_FINANCE_CLASS = fc.ddlid AND fc.sysid = 100021 AND fc.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_ddl_dtl lb ON b.LBFL = lb.ddlid AND lb.sysid = 100866 AND lb.valid_to = DATE('9999-12-31')
LEFT JOIN ods_erp.zx_busiscope_type bt ON b.scopetypeid = bt.scopetypeid; 