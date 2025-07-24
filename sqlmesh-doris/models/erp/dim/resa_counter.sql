MODEL (
  name sqlmesh_dim.erp_resa_counter,
  kind FULL,
  dialect doris,
  grain (counterid),
  physical_properties (
    unique_key = (valid_from, valid_to, counterid),
    distributed_by = (kind='HASH', expressions=counterid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "收银台维度表"
);

WITH ranked_data AS (
    SELECT counterid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY counterid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    counterid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.resa_counter
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
  a.counterid,
  a.counteropcode,
  a.counterno,
  a.countername,
  a.placepointid,
  b.placepointname,
  a.masterid,
  c.opcode AS masteropcode,
  c.employeename AS mastername,
  a.usestatus,
  a.inputmanid,
  d.employeename,
  a.credate
FROM ranked_data r
JOIN ods_erp.resa_counter a ON r.counterid = a.counterid AND r.DW_CREATETIME = a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_gpcs_placepoint b ON a.placepointid = b.placepointid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee c ON a.masterid = c.employeeid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee d ON a.inputmanid = d.employeeid AND d.valid_to = DATE('9999-12-31'); 