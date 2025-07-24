MODEL (
  name sqlmesh_dim.erp_pub_custom_set,
  kind FULL,
  dialect doris,
  grain setid,
  physical_properties (
    unique_key = (valid_from, valid_to, setid),
    distributed_by = (kind='HASH', expressions=setid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "客户集合维度表"
);

WITH ranked_data AS (
    SELECT setid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY setid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    setid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_custom_set
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
  a.setid,
  a.setopcode,
  a.setname,
  a.inputmanid,
  b.employeename AS inputmanname,
  a.credate,
  a.usestatus,
  a.entryid,
  c.entryname
FROM ranked_data a1
JOIN ods_erp.pub_custom_set a ON a1.setid=a.setid AND a1.DW_CREATETIME=a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_employee b ON a.inputmanid = b.employeeid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_entry c ON a.entryid = c.entryid AND c.valid_to = DATE('9999-12-31'); 