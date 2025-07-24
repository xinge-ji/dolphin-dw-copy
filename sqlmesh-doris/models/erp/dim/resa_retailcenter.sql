MODEL (
  name sqlmesh_dim.erp_resa_retailcenter,
  kind FULL,
  dialect doris,
  grain retailcenterid,
  physical_properties (
    unique_key = (valid_from, valid_to, retailcenterid),
    distributed_by = (kind='HASH', expressions=retailcenterid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "零售中心维度表"
);

WITH ranked_data AS (
    SELECT retailcenterid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY retailcenterid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    retailcenterid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.resa_retailcenter
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
  a.retailcenterid,
  a.retailcenteropcode,
  a.retailcentername,
  a.credate,
  a.inputmanid,
  b.employeename,
  a.storageid,
  c.opcode AS storageopcode,
  c.storagename,
  a.memo,
  a.direct_stid,
  d.opcode AS direct_stopcode,
  d.storagename AS direct_stname,
  IFNULl(a.insautoup,0) AS insautoup,
  a.invtype
FROM ranked_data a1
JOIN ods_erp.resa_retailcenter a ON a1.retailcenterid=a.retailcenterid AND a1.DW_CREATETIME=a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_employee b ON a.inputmanid = b.employeeid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_bms_st_def c ON a.storageid = c.storageid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_bms_st_def d ON a.direct_stid = d.storageid AND d.valid_to = DATE('9999-12-31'); 