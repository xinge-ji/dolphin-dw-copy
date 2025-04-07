DROP TABLE IF EXISTS dim.entry;
CREATE TABLE dim.entry (
  entryid bigint COMMENT "独立单元ID",
  dw_starttime datetime COMMENT '数据开始时间',
  dw_endtime datetime COMMENT '数据结束时间',
  is_active tinyint COMMENT '是否有效',
  entry_name varchar COMMENT "独立单元名称",
  is_disabled tinyint default "0" COMMENT "是否禁用",
  cityid bigint COMMENT "城市ID",
  city_name varchar COMMENT "城市名称",
  provinceid bigint COMMENT "省份ID",
  province_name varchar COMMENT "省份名称",
  area_name varchar COMMENT "区域名称",
  caiwu_level1 varchar COMMENT "财务一级分类",
  caiwu_level2 varchar COMMENT "财务二级分类",
  org_id bigint COMMENT "电商ID"
) UNIQUE KEY(entryid, dw_starttime) DISTRIBUTED BY HASH(entryid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 使用CTE和JOIN一次性插入所有数据
INSERT INTO dim.entry (
  entryid,
  dw_starttime,
  dw_endtime,
  is_active,
  entry_name,
  is_disabled,
  cityid,
  city_name,
  provinceid,
  province_name,
  area_name,
  caiwu_level1,
  caiwu_level2,
  org_id
)
WITH ranked_pub_entry AS (
  SELECT
    entryid,
    dw_createtime,
    dw_updatetime,
    is_active,
    entryname,
    IFNULL(sfty, 0) as sfty,
    cityid,
    area,
    ROW_NUMBER() OVER (
      PARTITION BY entryid
      ORDER BY
        dw_createtime
    ) AS record_seq,
    LEAD(date(dw_createtime),1,NULL) OVER (
      PARTITION BY entryid
      ORDER BY
        dw_createtime
    ) AS next_start_time
  FROM
    ods_erp.pub_entry
),

base_entry AS (
  SELECT
    entryid,
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
    entryname as entry_name,
    sfty as is_disabled,
    cityid,
    CASE 
      WHEN area=1 THEN '福建'
      WHEN area=2 THEN '江西'
      WHEN area=3 THEN '四川'
      WHEN area=4 THEN '海南'
      ELSE 'UNKNOWN'
    END as area_name
  FROM
    ranked_pub_entry
),

-- 获取城市省份信息
city_province_info AS (
  SELECT 
    e.entryid,
    e.dw_starttime,
    e.dw_endtime,
    e.is_active,
    e.entry_name,
    e.is_disabled,
    e.cityid,
    COALESCE(pc.city_name, 'UNKNOWN') AS city_name,
    COALESCE(pc.provinceid, NULL) AS provinceid,
    COALESCE(pc.province_name, 'UNKNOWN') AS province_name,
    e.area_name
  FROM 
    base_entry e
    LEFT JOIN (
      SELECT pc.*,
        ROW_NUMBER() OVER (PARTITION BY pc.cityid ORDER BY pc.dw_starttime DESC) as rn
      FROM dim.province_city pc
      WHERE pc.is_active = 1
    ) pc ON e.cityid = pc.cityid AND pc.rn = 1
),

-- 处理缺失城市信息的记录
city_match AS (
  SELECT 
    e.entryid,
    e.dw_starttime,
    e.dw_endtime,
    e.is_active,
    e.entry_name,
    e.is_disabled,
    COALESCE(
      CASE WHEN e.entry_name LIKE '%阆中%' AND e.cityid IS NULL THEN 838 ELSE e.cityid END,
      pc1.cityid,
      e.cityid
    ) AS cityid,
    COALESCE(
      CASE WHEN e.entry_name LIKE '%阆中%' AND e.cityid IS NULL THEN '南充市' ELSE e.city_name END,
      pc1.city_name,
      e.city_name
    ) AS city_name,
    COALESCE(
      CASE WHEN e.entry_name LIKE '%阆中%' AND e.cityid IS NULL THEN 163 ELSE e.provinceid END,
      pc1.provinceid,
      pc2.provinceid,
      e.provinceid
    ) AS provinceid,
    COALESCE(
      CASE WHEN e.entry_name LIKE '%阆中%' AND e.cityid IS NULL THEN '四川省' ELSE e.province_name END,
      pc1.province_name,
      pc2.province_name,
      e.province_name
    ) AS province_name,
    e.area_name
  FROM 
    city_province_info e
    LEFT JOIN (
      SELECT pc.*,
        ROW_NUMBER() OVER (PARTITION BY pc.city_name ORDER BY pc.dw_starttime DESC) as rn
      FROM dim.province_city pc
      WHERE pc.is_active = 1
    ) pc1 ON e.cityid IS NULL AND e.entry_name LIKE CONCAT('%', REGEXP_REPLACE(pc1.city_name, '市|自治区|自治州|地区|特区', ''), '%') AND pc1.rn = 1
    LEFT JOIN (
      SELECT pc.*,
        ROW_NUMBER() OVER (PARTITION BY pc.provinceid ORDER BY pc.city_name) as rn
      FROM dim.province_city pc
      WHERE pc.is_active = 1
    ) pc2 ON e.provinceid IS NULL AND e.entry_name LIKE CONCAT('%', SUBSTRING(pc2.province_name, 1, 2), '%') AND pc2.rn = 1
),

-- 添加财务分类
finance_category AS (
  SELECT
    e.*,
    CASE
      WHEN e.entryid in (1,2,5,144,204,224,104,124,164) THEN '福建药品分销'
      WHEN e.entryid in (25,590,591,612,616,626,630,638,644,664,710,714,700) THEN '江西药品分销'
      WHEN e.entryid in (568,598,576,586,587,588,589,596,678,698,692,610,602,674,618,620,635,648,694,629,562,706) THEN '四川药品分销'
      WHEN e.entryid in (628,632,684) THEN '海南药品分销'
      WHEN e.entryid in (4,264,660,650,676,666,668,662,656,5,144,204,224,104,124,164,696) THEN '医疗器械'
      WHEN e.entryid in (64,550,702,614) THEN '燕来福'
      WHEN e.entryid in (600) THEN '毫州中药'
      WHEN e.entryid in (6,686,304,424,284,444,528,658,244,636,682) THEN '零售'
      WHEN e.entryid in (640,654,708) THEN '新业态'
      WHEN e.entryid in (688,3,704) THEN '其他模块'
      ELSE 'UNKNOWN'
    END AS caiwu_level2
  FROM
    city_match e
),

-- 添加财务一级分类
finance_level1 AS (
  SELECT
    e.*,
    CASE
      WHEN e.caiwu_level2 in ('福建药品分销', '江西药品分销', '四川药品分销', '海南药品分销') THEN '药品分销'
      WHEN e.caiwu_level2 in ('医疗器械') THEN '医疗器械'
      WHEN e.caiwu_level2 in ('燕来福', '毫州中药') THEN '现代中医药'
      WHEN e.caiwu_level2 in ('零售') THEN '零售'
      WHEN e.caiwu_level2 in ('新业态') THEN '新业态'
      WHEN e.caiwu_level2 in ('其他模块') THEN '其他模块'
      ELSE 'UNKNOWN'
    END AS caiwu_level1
  FROM
    finance_category e
)

-- 最终查询，添加org_id
SELECT
  e.entryid,
  e.dw_starttime,
  e.dw_endtime,
  e.is_active,
  e.entry_name,
  e.is_disabled,
  e.cityid,
  e.city_name,
  e.provinceid,
  e.province_name,
  e.area_name,
  e.caiwu_level1,
  e.caiwu_level2,
  o.org_id
FROM
  finance_level1 e
  LEFT JOIN (
    SELECT 
      CASE WHEN erp_code=0 THEN '鹭燕（福建）集团' ELSE org_nm END AS org_nm, 
      org_id 
    FROM 
      ods_dsys.sys_org
  ) o ON e.entry_name = o.org_nm;

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_startdates ON dim.entry (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.entry (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.entry (is_active);
CREATE INDEX IF NOT EXISTS idx_entry_name ON dim.entry (entry_name);
CREATE INDEX IF NOT EXISTS idx_city_name ON dim.entry (city_name);
CREATE INDEX IF NOT EXISTS idx_province_name ON dim.entry (province_name);
CREATE INDEX IF NOT EXISTS idx_area_name ON dim.entry (area_name);