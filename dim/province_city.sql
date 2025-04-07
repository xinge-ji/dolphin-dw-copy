DROP TABLE IF EXISTS dim.province_city;
CREATE TABLE dim.province_city (
  cityid bigint COMMENT '城市ID',
  provinceid bigint COMMENT '省份ID',
  dw_starttime datetime COMMENT '数据生效时间',
  dw_endtime datetime COMMENT '数据失效时间',
  is_active tinyint COMMENT '是否有效',
  city_name varchar COMMENT '城市名称',
  province_name varchar COMMENT '省份名称'
) UNIQUE KEY(cityid, provinceid, dw_starttime) DISTRIBUTED BY HASH(cityid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

INSERT INTO
  dim.province_city (
    cityid,
    provinceid,
    dw_starttime,
    dw_endtime,
    is_active,
    city_name,
    province_name
  ) WITH ranked_city_province AS (
    SELECT
      c.cityid,
      c.provinceid,
      c.dw_createtime as city_createtime,
      c.dw_updatetime as city_updatetime,
      p.dw_createtime as province_createtime,
      p.dw_updatetime as province_updatetime,
      c.cityname,
      p.provincename,
      ROW_NUMBER() OVER (
        PARTITION BY c.cityid
        ORDER BY
          c.dw_createtime
      ) AS record_seq,
      LEAD(date(c.dw_createtime),1,NULL) OVER (
        PARTITION BY c.cityid
        ORDER BY c.dw_createtime
      ) AS next_start_time
    FROM
      ods_erp.pub_city c
      JOIN ods_erp.pub_province p ON c.provinceid = p.provinceid
  )
SELECT
  CAST(cityid as bigint),
  CAST(provinceid as bigint),
  CASE
    WHEN record_seq = 1 THEN LEAST(date('1970-01-01'), date(city_createtime))
    ELSE date(city_createtime)
  END AS dw_starttime,
  CASE
    WHEN next_start_time IS NOT NULL THEN next_start_time
    WHEN city_createtime <> city_updatetime THEN date(city_updatetime)
    ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
  END AS dw_endtime,
  CASE
    WHEN next_start_time IS NOT NULL THEN 0
    WHEN city_createtime <> city_updatetime THEN 0
    ELSE 1
  END AS is_active,
  cityname,
  provincename
FROM
  ranked_city_province;

-- 添加索引优化查询性能
CREATE INDEX IF NOT EXISTS idx_startdates ON dim.province_city (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.province_city (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.province_city (is_active);
CREATE INDEX IF NOT EXISTS idx_provinceid ON dim.province_city (provinceid);