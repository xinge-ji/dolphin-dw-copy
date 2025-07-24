MODEL (
  name sqlmesh_dim.erp_pub_company,
  kind FULL,
  dialect doris,
  grain companyid,
  physical_properties (
    unique_key = (valid_from, valid_to, companyid),
    distributed_by = (kind='HASH', expressions=companyid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "公司维度表"
);

WITH ranked_data AS (
    SELECT COMPANYID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY COMPANYID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    COMPANYID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_company
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
  b.companyid,
  b.companyopcode,
  b.companypinyin,
  b.companyname,
  b.parentcompanyid,
  pc.companyopcode AS parentcompanyopcode,
  pc.companyname AS parentcompanyname,
  b.corpcode,
  nvl(b.selfflag,0) AS selfflag,
  b.credate,
  b.inputmanid,
  b.referencedcount,
  b.companymemo,
  nvl(b.supplyerflag,0) AS supplyerflag,
  nvl(b.qudeptflag,0) AS qudeptflag,
  nvl(b.customflag,0) AS customflag,
  nvl(b.factflag,0) AS factflag,
  nvl(b.transportflag,0) AS transportflag,
  nvl(b.deptsupply,0) AS deptsupply,
  nvl(b.deptsalesflag,0) AS deptsalesflag,
  nvl(b.deptstorerflag,0) AS deptstorerflag,
  nvl(b.bankflag,0) AS bankflag,
  b.financeno,
  nvl(s.registadd,c.registadd) AS regaddress,
  nvl(s.address,c.address) AS address,
  b.companyclass,
  b.zx_address,
  b.zx_fzrldhtjzrq,
  b.zx_zlfzr,
  b.zx_zlfzrldhtjzrq
FROM ranked_data a
JOIN ods_erp.pub_company b ON a.companyid=b.companyid AND a.DW_CREATETIME=b.DW_CREATETIME
LEFT JOIN ods_erp.pub_company pc ON b.parentcompanyid = pc.companyid
LEFT JOIN sqlmesh_dim.erp_pub_supplyer s ON b.companyid = s.supplyid AND s.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_customer c ON b.companyid = c.customid AND c.valid_to = DATE('9999-12-31')
