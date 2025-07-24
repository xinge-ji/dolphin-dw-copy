MODEL (
  name sqlmesh_dim.erp_zx_filiale_info_set,
  kind FULL,
  dialect doris,
  grain (seqid),
  physical_properties (
    unique_key = (valid_from, valid_to, seqid),
    distributed_by = (kind='HASH', expressions=seqid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "分支机构信息维度表",
  column_descriptions (
    regioncode = "江西省药品智慧监管平台二期 企业所在地区区域码",
    qyfzr = "江西省药品智慧监管平台二期 企业法人",
    zlfzr = "江西省药品智慧监管平台二期 质量负责人",
    longitude = "江西省药品智慧监管平台二期 经度",
    latitude = "江西省药品智慧监管平台二期 纬度",
    kpcode = "江西省药品智慧监管平台二期 极速开票代码",
    bankid = "江西省药品智慧监管平台二期 开户行ID"
  )
);

WITH ranked_data AS (
    SELECT seqid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY seqid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    seqid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.zx_filiale_info_set
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
  a.seqid,
  a.entryid,
  b.entryname,
  a.registeredaddress,
  a.corporation,
  a.storageaddress,
  a.tel,
  a.fax,
  a.subank,
  a.subanknum,
  a.sabank,
  a.sabanknum,
  a.taxregisternum,
  a.stman,
  a.stmantel,
  a.postcode,
  a.memo,
  a.regioncode,
  a.qyfzr,
  a.zlfzr,
  a.longitude,
  a.latitude,
  a.kpcode,
  a.bankid,
  p.bankname,
  p.accno
FROM ranked_data r
JOIN ods_erp.zx_filiale_info_set a ON r.seqid = a.seqid AND r.DW_CREATETIME = a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_entry b ON a.entryid = b.entryid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_bank p ON a.bankid = p.bankid AND p.valid_to = DATE('9999-12-31'); 