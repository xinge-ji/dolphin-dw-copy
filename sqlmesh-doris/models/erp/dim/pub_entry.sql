MODEL (
  name sqlmesh_dim.erp_pub_entry,
  kind FULL,
  dialect doris,
  grain entryid,
  physical_properties (
    unique_key = (valid_from, valid_to, entryid),
    distributed_by = (kind='HASH', expressions=entryid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "独立单元",
  column_descriptions (
    ESHOPFLAG = "电商标志",
    sfty = "是否停用,用于其他报表过滤停用独立单元",
    ywmk = "业务模块id",
    ywmkname = "业务模块",
    zx_nbgylcomid = "内部供应链相关单位ID",
    cityname = "城市名称",
    provincename = "省份名称",
    caiwu_level1 = "财务一级分类",
    caiwu_level2 = "财务二级分类"
  )
);

WITH ranked_data AS (
    SELECT ENTRYID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY ENTRYID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    ENTRYID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_entry
),
base_entry AS (
  SELECT
    CASE WHEN a.record_seq = 1 THEN DATE('1970-01-01') ELSE date(b.DW_CREATETIME) END AS valid_from,
    CASE WHEN a.next_start_time IS NOT NULL THEN a.next_start_time ELSE DATE('9999-12-31') END AS valid_to,
    b.entryid,
    b.entryname,
    b.entrycompanyid,
    d.companyname AS entrycompanyname,
    b.legalentryid,
    c.entryname legalentryname,
    b.goodsownerid,
    b.ischain,
    b.usestatus,
    b.ESHOPFLAG,
    b.area area,
    IFNULL(f.ddlname, '其他') AS areaname,
    IFNULL(b.sfty, 0) sfty,
    b.entryxqday,
    b.ZX_ZRYXYEDFLAG,
    b.cityid,
    b.ywmk,
    IFNULL(g.ddlname, '其他') ywmkname,
    b.zx_sf_accountno,
    b.zx_jd_accountno,
    b.zx_nbgylcomid
  FROM ranked_data a
  JOIN ods_erp.pub_entry b ON a.ENTRYID=b.ENTRYID AND a.DW_CREATETIME=b.DW_CREATETIME
  LEFT JOIN ods_erp.pub_entry c ON b.legalentryid = c.entryid
  LEFT JOIN sqlmesh_dim.erp_pub_company d ON b.entrycompanyid = d.companyid
  LEFT JOIN (Select * From sqlmesh_dim.erp_pub_ddl_dtl Where sysid = 100646 AND valid_to=DATE('9999-12-31')) f ON b.area = f.ddlid
  LEFT JOIN (Select * From sqlmesh_dim.erp_pub_ddl_dtl Where sysid = 101966 AND valid_to=DATE('9999-12-31')) g ON b.ywmk = g.ddlid
),
city_province_info AS (
  SELECT 
    e.*,
    pc.cityname,
    pc.provinceid,
    pc.provincename
  FROM 
    base_entry e
    LEFT JOIN (
      SELECT * FROM sqlmesh_dim.erp_pub_city WHERE valid_to=DATE('9999-12-31')
    ) pc ON e.cityid = pc.cityid
),
city_match AS (
  SELECT 
    e.*,
    COALESCE(
      CASE WHEN e.entryname LIKE '%阆中%' AND e.cityid IS NULL THEN 838 ELSE e.cityid END,
      pc1.cityid,
      e.cityid
    ) AS cityid_final,
    COALESCE(
      CASE WHEN e.entryname LIKE '%阆中%' AND e.cityid IS NULL THEN '南充市' ELSE e.cityname END,
      pc1.cityname,
      e.cityname
    ) AS cityname_final,
    COALESCE(
      CASE WHEN e.entryname LIKE '%阆中%' AND e.cityid IS NULL THEN 163 ELSE e.provinceid END,
      pc1.provinceid,
      pc2.provinceid,
      e.provinceid
    ) AS provinceid_final,
    COALESCE(
      CASE WHEN e.entryname LIKE '%阆中%' AND e.cityid IS NULL THEN '四川省' ELSE e.provincename END,
      pc1.provincename,
      pc2.provincename,
      e.provincename
    ) AS provincename_final
  FROM 
    city_province_info e
    LEFT JOIN (
      SELECT * FROM sqlmesh_dim.erp_pub_city WHERE valid_to=DATE('9999-12-31')
    ) pc1 ON e.cityid IS NULL AND e.entryname LIKE CONCAT('%', REGEXP_REPLACE(pc1.cityname, '市|自治区|自治州|地区|特区', ''), '%')
    LEFT JOIN (
      SELECT * FROM sqlmesh_dim.erp_pub_city WHERE valid_to=DATE('9999-12-31')
    ) pc2 ON e.provinceid IS NULL AND e.entryname LIKE CONCAT('%', SUBSTRING(pc2.provincename, 1, 2), '%')
),
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
      WHEN e.entryid in (564, 600) THEN '毫州中药'
      WHEN e.entryid in (6,686,304,424,284,444,528,658,244,636,682) THEN '零售'
      WHEN e.entryid in (640,654,708) THEN '新业态'
      WHEN e.entryid in (688,3,704) THEN '其他模块'
      ELSE 'UNKNOWN'
    END AS caiwu_level2
  FROM
    city_match e
),
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
SELECT 
  valid_from,
  valid_to,
  entryid,
  entryname,
  entrycompanyid,
  entrycompanyname,
  legalentryid,
  legalentryname,
  goodsownerid,
  ischain,
  usestatus,
  ESHOPFLAG,
  area,
  areaname,
  sfty,
  entryxqday,
  ZX_ZRYXYEDFLAG,
  cityid_final as cityid,
  CASE
    WHEN provincename_final = '海南省' AND (cityname_final IS NULL OR cityname_final = '') THEN '海口市'
    WHEN cityname_final IS NULL OR cityname_final = '' THEN '其他'
    ELSE cityname_final
  END as cityname,
  provinceid_final as provinceid,
  IFNULL(provincename_final, '其他') as provincename,
  ywmk,
  ywmkname,
  zx_sf_accountno,
  zx_jd_accountno,
  zx_nbgylcomid,
  caiwu_level1,
  caiwu_level2
FROM finance_level1;
