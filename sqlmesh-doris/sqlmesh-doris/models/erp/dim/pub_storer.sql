MODEL (
  name sqlmesh_dim.erp_pub_storer,
  kind FULL,
  dialect doris,
  grain storerid,
  physical_properties (
    unique_key = (valid_from, valid_to, storerid),
    distributed_by = (kind='HASH', expressions=storerid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "仓库维度表"
);

WITH ranked_data AS (
    SELECT storerid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY storerid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    storerid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_storer
)
SELECT
  CASE
    WHEN a1.record_seq = 1 
    THEN DATE('1970-01-01')
    ELSE date(a.DW_CREATETIME)
  END AS valid_from,
  CASE
    WHEN a1.next_start_time IS NOT NULL THEN a1.next_start_time
    ELSE DATE('9999-12-31')
  END AS valid_to,
  a.storerid,
  a.storeropcode,
  a.storerno,
  c.companyname AS storername,
  a.masterid,
  b.opcode AS masteropcode,
  b.employeename AS mastername,
  a.wmsflag,
  a.usestatus
FROM ranked_data a1
JOIN ods_erp.pub_storer a ON a1.storerid=a.storerid AND a1.DW_CREATETIME=a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_employee b ON a.masterid = b.employeeid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_company c ON a.storerid = c.companyid AND c.valid_to = DATE('9999-12-31'); 