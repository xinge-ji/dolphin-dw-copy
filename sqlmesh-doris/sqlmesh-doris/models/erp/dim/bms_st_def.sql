MODEL (
  name sqlmesh_dim.erp_bms_st_def,
  kind FULL,
  dialect doris,
  grain storageid,
  physical_properties (
    unique_key = (valid_from, valid_to, storageid),
    distributed_by = (kind='HASH', expressions=storageid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "仓库维度表"
);

WITH ranked_data AS (
    SELECT storageid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY storageid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    storageid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.bms_st_def
)
SELECT
  CASE
    WHEN a1.record_seq = 1 
    THEN DATE('1970-01-01')
    ELSE date(b.DW_CREATETIME)
  END AS valid_from,
  CASE
    WHEN a1.next_start_time IS NOT NULL THEN a1.next_start_time
    ELSE DATE('9999-12-31')
  END AS valid_to,
  b.storageid,
  b.opcode,
  b.storageno,
  b.storagename,
  b.whid,
  b.goodsdtlflag,
  b.batchflag,
  b.lotflag,
  b.posflag,
  b.phystoreid,
  b.memo,
  b.procflag,
  b.autocrtflag,
  b.entryid,
  e.entryname,
  c.companyname AS phystorename,
  b.presflag,
  b.empid,
  d.employeename AS empname,
  d.opcode AS empopcode,
  b.eraflag,
  b.wmsflag,
  b.zx_goodstatusid,
  b.ESHOPFLAG,
  b.ZX_NOSUFLAG,
  b.ocflag,
  b.zd_flag
FROM ranked_data a1
JOIN ods_erp.bms_st_def b ON a1.storageid=b.storageid AND a1.DW_CREATETIME=b.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_storer s ON b.phystoreid = s.storerid AND s.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_entry e ON b.entryid = e.entryid AND e.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee d ON b.empid = d.employeeid AND d.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_company c ON b.phystoreid = c.companyid AND c.valid_to = DATE('9999-12-31')