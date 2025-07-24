MODEL (
  name sqlmesh_dim.erp_pub_supplyer,
  kind FULL,
  dialect doris,
  grain supplyid,
  physical_properties (
    unique_key = (valid_from, valid_to, supplyid),
    distributed_by = (kind='HASH', expressions=supplyid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "供应商维度表"
);

WITH ranked_data AS (
    SELECT SUPPLYID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY SUPPLYID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    SUPPLYID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_supplyer
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
  b.supplyid,
  b.supplyopcode,
  b.supplyno,
  b.supplypinyin,
  b.corpcode,
  b.supplyname,
  b.usestatus,
  nvl(b.importflag,0) AS importflag,
  b.fmid,
  c.fmopcode,
  c.fmname,
  c.fmrate,
  b.financeno,
  b.supplymemo,
  b.credate,
  b.inputmanid,
  b.cityid,
  ci.CITYNAME,
  ci.PROVINCEID,
  ci.provincename,
  b.countryid,
  co.countryopcode,
  co.countryname,
  b.registadd,
  b.address,
  nvl(b.isfarmerflag,0) AS isfarmerflag,
  b.gspflag,
  b.supplyclass,
  b.gmpflag,
  b.categoryid
FROM ranked_data a
JOIN ods_erp.pub_supplyer b ON a.supplyid=b.supplyid AND a.DW_CREATETIME=b.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_formoney c ON b.fmid = c.fmid AND c.valid_to=DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_city ci ON b.cityid = ci.CITYID AND ci.valid_to=DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_country co ON b.countryid = co.countryid AND co.valid_to=DATE('9999-12-31')