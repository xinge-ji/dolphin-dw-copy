MODEL (
  name sqlmesh_dim.erp_pub_factory,
  kind FULL,
  dialect doris,
  grain factoryid,
  physical_properties (
    unique_key = (valid_from, valid_to, factoryid),
    distributed_by = (kind='HASH', expressions=factoryid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "厂家维度表",
  column_descriptions (
    factoryclass = "厂家类型",
    brandintegration = "厂牌整合",
    zx_scxkzh = "生产许可证号"
  )
);

WITH ranked_data AS (
    SELECT FACTORYID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY FACTORYID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    FACTORYID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_factory
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
  b.factoryid,
  b.factoryopcode,
  b.factoryno,
  b.factorypinyin,
  b.factoryname,
  b.corpcode,
  b.usestatus,
  b.memo,
  b.credate,
  b.inputmanid,
  b.cityid,
  ci.cityname,
  ci.provinceid,
  ci.provincename,
  b.countryid,
  co.countryopcode,
  co.countryname,
  b.factoryclass,
  b.brandintegration,
  b.zx_scxkzh
FROM ranked_data a
JOIN ods_erp.pub_factory b ON a.factoryid=b.factoryid AND a.DW_CREATETIME=b.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_city ci ON b.cityid = ci.cityid AND ci.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_country co ON b.countryid = co.countryid AND co.valid_to = DATE('9999-12-31'); 