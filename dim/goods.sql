DROP TABLE IF EXISTS dim.goods;

CREATE TABLE
  dim.goods (
    goodsid bigint COMMENT '商品ID',
    dw_starttime datetime COMMENT '数据开始时间',
    dw_endtime datetime COMMENT '数据结束时间',
    is_active tinyint COMMENT '是否有效',
    create_date datetime COMMENT '创建时间',
    goods_name varchar COMMENT '商品名称',
    supply_taxrate decimal(10,4) COMMENT '进项税率',
    sales_taxrate decimal(10,4) COMMENT '销项税率',
    busiscope bigint COMMENT '业务范围',
    busiscope_name varchar(255) default "未分类" COMMENT '业务范围名称',
    nianbao_type varchar(255) default "未分类" COMMENT '年报类型',
    zx_finance_class bigint COMMENT '财务分类',
    finance_class_name varchar(255) default "未分类" COMMENT '财务分类名称',
    varietyid bigint COMMENT '商品小类ID',
    variety_level1_name varchar COMMENT '商品小类名称',
    variety_level2_name varchar COMMENT '商品中类名称',
    variety_level3_name varchar COMMENT '商品大类名称',
    group_manage_type varchar COMMENT '集团管理类型',
    qixie_class varchar COMMENT '器械分类',
    qixie_brandtype varchar COMMENT '器械品牌类型'
  ) UNIQUE KEY (goodsid, dw_starttime) DISTRIBUTED BY HASH (goodsid) PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
  );

INSERT INTO dim.goods (
    goodsid,
    dw_starttime,
    dw_endtime,
    is_active,
    create_date,
    goods_name,
    supply_taxrate,
    sales_taxrate,
    busiscope,
    busiscope_name,
    nianbao_type,
    zx_finance_class,
    finance_class_name,
    varietyid,
    variety_level1_name,
    variety_level2_name,
    variety_level3_name,
    group_manage_type,
    qixie_class,
    qixie_brandtype
)
WITH
  -- 获取字典表数据，合并三个字典查询为一个
  ddl_info AS (
    SELECT 
      ddlid,
      sysid,
      MIN(ddlname) as ddlname
    FROM 
      ods_erp.Pub_Ddl_Dtl
    WHERE 
      sysid IN (100021, 100104, 101386)
    group BY
      ddlid,
      sysid
  ),
  
  -- 获取基础商品数据
  ranked_goods AS (
    SELECT
      goodsid,
      dw_createtime,
      dw_updatetime,
      credate,
      goodsname,
      supplytaxrate,
      salestaxrate,
      busiscope,
      varietyid,
      zx_finance_class,
      groupmanagetype,
      qxbusiclass,
      brandtype,
      ROW_NUMBER() OVER (PARTITION BY goodsid ORDER BY dw_createtime) AS record_seq,
      LEAD(date(dw_createtime), 1, NULL) OVER (PARTITION BY goodsid ORDER BY dw_createtime) AS next_start_time
    FROM
      ods_erp.pub_goods
  ),
  
  -- 获取最新的业务范围和商品分类信息
  latest_busiscope AS (
    SELECT 
      scopedefid,
      scope_name,
      nianbao_type
    FROM (
      SELECT 
        scopedefid,
        scope_name,
        nianbao_type,
        ROW_NUMBER() OVER (PARTITION BY scopedefid ORDER BY dw_starttime DESC) AS rn
      FROM 
        dim.goods_busiscope
      WHERE 
        is_active = 1
    ) t WHERE rn = 1
  ),
  
  latest_variety AS (
    SELECT 
      varietyid,
      variety_level1_name,
      variety_level2_name,
      variety_level3_name
    FROM (
      SELECT 
        varietyid,
        variety_level1_name,
        variety_level2_name,
        variety_level3_name,
        ROW_NUMBER() OVER (PARTITION BY varietyid ORDER BY dw_starttime DESC) AS rn
      FROM 
        dim.goods_variety
      WHERE 
        is_active = 1
    ) t WHERE rn = 1
  ),
  
  -- 处理基础商品数据并关联所有信息
  goods_with_all_info AS (
    SELECT
      g.goodsid,
      CASE
        WHEN g.record_seq = 1 THEN LEAST(date('1970-01-01'), date(g.dw_createtime))
        ELSE date(g.dw_createtime)
      END AS dw_starttime,
      CASE
        WHEN g.next_start_time IS NOT NULL THEN g.next_start_time
        WHEN g.dw_createtime <> g.dw_updatetime THEN date(g.dw_updatetime)
        ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
      END AS dw_endtime,
      CASE
        WHEN g.next_start_time IS NOT NULL THEN 0
        WHEN g.dw_createtime <> g.dw_updatetime THEN 0
        ELSE 1
      END AS is_active,
      g.credate,
      g.goodsname,
      g.supplytaxrate,
      g.salestaxrate,
      g.busiscope,
      COALESCE(bs.scope_name, '未分类') AS busiscope_name,
      COALESCE(bs.nianbao_type, '未分类') AS nianbao_type,
      g.zx_finance_class,
      COALESCE(d1.ddlname, '未分类') AS finance_class_name,
      g.varietyid,
      v.variety_level1_name,
      v.variety_level2_name,
      v.variety_level3_name,
      COALESCE(d2.ddlname, NULL) AS group_manage_type,
      COALESCE(d3.ddlname, NULL) AS qixie_class,
      g.brandtype AS qixie_brandtype
    FROM
      ranked_goods g
      LEFT JOIN latest_busiscope bs ON g.busiscope = bs.scopedefid
      LEFT JOIN latest_variety v ON g.varietyid = v.varietyid
      LEFT JOIN ddl_info d1 ON g.zx_finance_class = d1.ddlid AND d1.sysid = 100021
      LEFT JOIN ddl_info d2 ON g.groupmanagetype = d2.ddlid AND d2.sysid = 100104
      LEFT JOIN ddl_info d3 ON g.qxbusiclass = d3.ddlid AND d3.sysid = 101386
  )
  
-- 最终查询
SELECT
  goodsid,
  dw_starttime,
  dw_endtime,
  is_active,
  credate AS create_date,
  goodsname AS goods_name,
  supplytaxrate AS supply_taxrate,
  salestaxrate AS sales_taxrate,
  busiscope,
  busiscope_name,
  nianbao_type,
  zx_finance_class,
  finance_class_name,
  varietyid,
  variety_level1_name,
  variety_level2_name,
  variety_level3_name,
  group_manage_type,
  qixie_class,
  qixie_brandtype
FROM
  goods_with_all_info;

CREATE INDEX IF NOT EXISTS idx_startdates ON dim.goods (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.goods (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.goods (is_active);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dim.goods (goodsid);
CREATE INDEX IF NOT EXISTS idx_varietyid ON dim.goods (varietyid);
CREATE INDEX IF NOT EXISTS idx_busiscope ON dim.goods (busiscope);