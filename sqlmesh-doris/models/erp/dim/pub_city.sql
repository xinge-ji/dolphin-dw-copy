MODEL (
  name sqlmesh_dim.erp_pub_city,
  kind FULL,
  dialect doris,
  grain cityid,
  physical_properties (
    unique_key = (valid_from, valid_to, cityid),
    distributed_by = (kind='HASH', expressions=cityid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "城市"
);

WITH ranked_data AS (
    SELECT CITYID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY CITYID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    CITYID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_city
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
b.CITYID,
b.CITYNAME,
c.PROVINCEID,
c.PROVINCENAME
FROM ranked_data a
JOIN ods_erp.pub_city b ON a.CITYID=b.CITYID AND a.DW_CREATETIME=b.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_province c ON c.PROVINCEID=b.PROVINCEID AND c.valid_to=DATE('9999-12-31')
    