DROP TABLE IF EXISTS dim.retail_placepoint;
CREATE TABLE dim.retail_placepoint (
  placepointid bigint COMMENT '门店ID',
  dw_starttime datetime COMMENT '数据开始时间',
  dw_endtime datetime COMMENT '数据结束时间',
  is_active tinyint COMMENT '是否有效',
  placepoint_name varchar COMMENT '门店名称',
  placepoint_no varchar COMMENT '门店类型：院边店/社区店',
  create_date datetime COMMENT '创建时间',
  storageid bigint COMMENT '仓库ID',
  areadocid bigint COMMENT '区域ID',
  area_name varchar COMMENT '区域名称',
  retailcenterid bigint COMMENT '所属中心ID',
  entryid bigint COMMENT '所属独立单元ID',
  entry_name varchar COMMENT '所属独立单元名称',
  province_name varchar COMMENT '所属省名称',
  city_name varchar COMMENT '所属市名称'
) UNIQUE KEY(placepointid, dw_starttime) DISTRIBUTED BY HASH(placepointid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 使用CTE和JOIN一次性插入所有数据，包括关联的区域和独立单元信息
INSERT INTO dim.retail_placepoint (
  placepointid,
  dw_starttime,
  dw_endtime,
  is_active,
  placepoint_name,
  placepoint_no,
  create_date,
  storageid,
  areadocid,
  area_name,
  retailcenterid,
  entryid,
  entry_name,
  province_name,
  city_name
)
WITH ranked_retail_placepoint AS (
  SELECT
    placepointid,
    dw_createtime,
    dw_updatetime,
    placepointname,
    placepointno,
    credate,
    storageid,
    areadocid,
    retailcenterid,
    entryid,
    ROW_NUMBER() OVER (
      PARTITION BY placepointid
      ORDER BY
        dw_createtime
    ) AS record_seq,
    LEAD(date(dw_createtime),1,NULL) OVER (
      PARTITION BY placepointid
      ORDER BY
        dw_createtime
    ) AS next_start_time
  FROM
    ods_erp.gpcs_placepoint
),

base_placepoint AS (
  SELECT
    placepointid,
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
    placepointname AS placepoint_name,
    IFNULL(trim(placepointno), '未分类') AS placepoint_no,
    credate AS create_date,
    storageid,
    areadocid,
    retailcenterid,
    entryid
  FROM
    ranked_retail_placepoint
),

-- 获取区域信息
area_info AS (
  SELECT 
    a.areadocid,
    a.area_name,
    a.dw_starttime AS area_starttime
  FROM 
    dim.retail_placepoint_area a
  WHERE 
    a.is_active = 1
),

-- 获取独立单元信息
entry_info AS (
  SELECT 
    e.entryid,
    e.entry_name,
    e.province_name,
    e.city_name,
    e.dw_starttime AS entry_starttime
  FROM 
    dim.entry e
  WHERE 
    e.is_active = 1
),

-- 关联区域和独立单元信息
placepoint_with_relations AS (
  SELECT
    p.placepointid,
    p.dw_starttime,
    p.dw_endtime,
    p.is_active,
    p.placepoint_name,
    p.placepoint_no,
    p.create_date,
    p.storageid,
    p.areadocid,
    a.area_name,
    p.retailcenterid,
    p.entryid,
    e.entry_name,
    e.province_name,
    e.city_name
  FROM
    base_placepoint p
    LEFT JOIN (
      SELECT 
        a.*,
        ROW_NUMBER() OVER (PARTITION BY a.areadocid ORDER BY a.area_starttime DESC) as rn
      FROM 
        area_info a
    ) a ON p.areadocid = a.areadocid AND a.rn = 1
    LEFT JOIN (
      SELECT 
        e.*,
        ROW_NUMBER() OVER (PARTITION BY e.entryid ORDER BY e.entry_starttime DESC) as rn
      FROM 
        entry_info e
    ) e ON p.entryid = e.entryid AND e.rn = 1
)

-- 最终查询
SELECT
  placepointid,
  dw_starttime,
  dw_endtime,
  is_active,
  placepoint_name,
  placepoint_no,
  create_date,
  storageid,
  areadocid,
  COALESCE(area_name, 'UNKNOWN') AS area_name,
  retailcenterid,
  entryid,
  COALESCE(entry_name, 'UNKNOWN') AS entry_name,
  COALESCE(province_name, 'UNKNOWN') AS province_name,
  COALESCE(city_name, 'UNKNOWN') AS city_name
FROM
  placepoint_with_relations;

CREATE INDEX IF NOT EXISTS idx_startdates ON dim.retail_placepoint (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.retail_placepoint (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.retail_placepoint (is_active);