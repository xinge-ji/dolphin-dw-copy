DROP TABLE IF EXISTS dim.retail_placepoint_area;
CREATE TABLE dim.retail_placepoint_area (
  areadocid bigint COMMENT '区域id',
  dw_starttime datetime COMMENT '数据开始时间',
  dw_endtime datetime COMMENT '数据结束时间',
  is_active tinyint COMMENT '是否有效',
  create_date datetime COMMENT '创建时间',
  area_name varchar COMMENT '区域名称'
) UNIQUE KEY(areadocid, dw_starttime) DISTRIBUTED BY HASH(areadocid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
      "in_memory" = "false",
      "storage_format" = "V2",
      "disable_auto_compaction" = "false"
);
INSERT INTO
  dim.retail_placepoint_area (
    areadocid,
    dw_starttime,
    dw_endtime,
    is_active,
    create_date,
    area_name
  ) WITH ranked_retail_placepoint_area AS (
    SELECT
      areadocid,
      dw_createtime,
      dw_updatetime,
      is_active,
      credate,
      AREANAME,
      ROW_NUMBER() OVER (
        PARTITION BY areadocid
        ORDER BY
          dw_createtime
      ) AS record_seq,
      LEAD(date(dw_createtime),1,NULL) OVER (
        PARTITION BY areadocid
          ORDER BY dw_createtime
      ) AS next_start_time
    FROM
      ods_erp.GPCS_SHOP_AREA_DOC
  )
SELECT
  areadocid,
  CASE
    WHEN record_seq = 1 THEN LEAST(date('1970-01-01'), date(dw_createtime))
    ELSE date(dw_createtime)
  END AS dw_starttime,
  CASE
    WHEN next_start_time IS NOT NULL THEN next_start_time
    WHEN dw_createtime <> dw_updatetime THEN date(dw_updatetime)
    ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
  END AS dw_endtime,
  CASE
    WHEN next_start_time IS NOT NULL THEN 0
    WHEN dw_createtime <> dw_updatetime THEN 0
    ELSE 1
  END AS is_active,
  credate,
  AREANAME
FROM
  ranked_retail_placepoint_area;

CREATE INDEX IF NOT EXISTS idx_startdates ON dim.retail_placepoint_area (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.retail_placepoint_area (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.retail_placepoint_area (is_active);
