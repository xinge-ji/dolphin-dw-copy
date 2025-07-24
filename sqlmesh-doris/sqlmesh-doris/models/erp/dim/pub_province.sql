MODEL (
  name sqlmesh_dim.erp_pub_province,
  kind FULL,
  dialect doris,
  grain provinceid,
  physical_properties (
    unique_key = (valid_from, valid_to, provinceid),
    distributed_by = (kind='HASH', expressions=provinceid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "省份"
);

WITH ranked_data AS (
    SELECT PROVINCEID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY PROVINCEID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    PROVINCEID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_province
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
b.PROVINCEID,
b.IS_ACTIVE,
CASE
    WHEN b.PROVINCENAME = '新疆维吾尔自治区' THEN '新疆维吾尔族自治区'
    WHEN b.PROVINCENAME = '澳门特区' THEN '澳门特别行政区'
    WHEN b.PROVINCENAME = '香港特区' THEN '香港特别行政区'
    ELSE b.PROVINCENAME
END AS PROVINCENAME
FROM ranked_data a
JOIN ods_erp.pub_province b ON a.PROVINCEID=b.PROVINCEID AND a.DW_CREATETIME=b.DW_CREATETIME

    