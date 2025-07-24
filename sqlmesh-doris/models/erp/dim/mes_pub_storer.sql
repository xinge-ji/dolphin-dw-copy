MODEL (
  name sqlmesh_dim.erp_mes_pub_storer,
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
  description "WMS仓库部门视图，带递归部门独立单元ID逻辑（3层）",
  column_descriptions (
    storerid = "仓库部门ID",
    wmsflag = "启用WMS标志",
    usestatus = "启用状态（0停用 1启用）",
    entryid = "独立单元ID"
  )
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
),
ancestor_entry AS (
  SELECT
    a.storerid,
    CASE
      WHEN c2.selfflag = 1 THEN c2.companyid
      WHEN c3.selfflag = 1 THEN c3.companyid
      WHEN c4.selfflag = 1 THEN c4.companyid
      ELSE NULL
    END AS ancestor_companyid
  FROM ods_erp.pub_storer a
  LEFT JOIN sqlmesh_dim.erp_pub_company c1 ON a.storerid = c1.companyid AND c1.valid_to = DATE('9999-12-31')
  LEFT JOIN sqlmesh_dim.erp_pub_company c2 ON c1.parentcompanyid = c2.companyid AND c2.valid_to = DATE('9999-12-31')
  LEFT JOIN sqlmesh_dim.erp_pub_company c3 ON c2.parentcompanyid = c3.companyid AND c3.valid_to = DATE('9999-12-31')
  LEFT JOIN sqlmesh_dim.erp_pub_company c4 ON c3.parentcompanyid = c4.companyid AND c3.valid_to = DATE('9999-12-31')
),
entryid_lookup AS (
  SELECT
    a.storerid,
    MAX(e.entryid) AS entryid
  FROM ancestor_entry a
  LEFT JOIN sqlmesh_dim.erp_pub_entry e ON a.ancestor_companyid = e.entrycompanyid AND e.valid_to = DATE('9999-12-31')
  GROUP BY a.storerid
),
mes_entry_exists AS (
  SELECT DISTINCT entryid FROM ods_erp.mes_entry
)
SELECT
  CASE
    WHEN r.record_seq = 1 
    THEN DATE('1970-01-01')
    ELSE date(s.DW_CREATETIME)
  END AS valid_from,
  CASE
    WHEN r.next_start_time IS NOT NULL THEN r.next_start_time
    ELSE DATE('9999-12-31')
  END AS valid_to,
  s.storerid,
  s.wmsflag,
  s.usestatus,
  eid.entryid
FROM ranked_data r
JOIN ods_erp.pub_storer s ON r.storerid = s.storerid AND r.DW_CREATETIME = s.DW_CREATETIME
JOIN entryid_lookup eid ON s.storerid = eid.storerid
JOIN mes_entry_exists me ON eid.entryid = me.entryid; 