MODEL (
  name sqlmesh_dim.erp_zx_busiscope_type,
  kind FULL,
  dialect doris,
  grain (SCOPETYPEID as erp_zx_busiscope_type_id),
  physical_properties (
    unique_key = (valid_from, valid_to, SCOPETYPEID),
    distributed_by = (kind='HASH', expressions=SCOPETYPEID, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "数据字典"
);

WITH ranked_data AS (
    SELECT SCOPETYPEID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY SCOPETYPEID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    SCOPETYPEID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.zx_busiscope_type
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
b.SCOPETYPEID,
b.SCOPETYPE
FROM ranked_data a
JOIN ods_erp.zx_busiscope_type b ON a.SCOPETYPEID=b.SCOPETYPEID AND a.DW_CREATETIME=b.DW_CREATETIME