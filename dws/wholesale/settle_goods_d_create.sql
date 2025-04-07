DROP TABLE IF EXISTS dws.wholesale_settle_goods_d;

-- 创建批发订单结算商品日汇总表
CREATE TABLE dws.wholesale_settle_goods_d (
    -- 维度信息
    stat_date DATE COMMENT "业务日期",
    goodsid bigint COMMENT "商品ID",
    entryid bigint COMMENT "独立单元ID",
    customid bigint COMMENT "客户ID",
    
    -- 集采信息
    is_jicai_zhongxuan tinyint COMMENT "是否集采中选(0:否, 1:是)",

    -- 组织信息
    entry_name varchar COMMENT "独立单元名称",
    province_name varchar COMMENT "独立单元省份名称",
    city_name varchar COMMENT "独立单元城市名称",
    area_name varchar COMMENT "独立单元地区名称",
    caiwu_level1 varchar COMMENT "集团模块一级",
    caiwu_level2 varchar COMMENT "集团模块二级",

    -- 客户信息
    customer_name varchar COMMENT "客户名称",
    customertype_name varchar COMMENT "客户类型名称",
    customertype_group varchar COMMENT "客户类型分组",
    customer_financeclass_name varchar COMMENT "客户财务类别名称",
    
    -- 商品信息
    nianbao_type varchar COMMENT "商品年报类型",
    qixie_class varchar COMMENT "器械分类",
    qixie_brandtype varchar COMMENT "器械品牌类型",
    leibiebeizhu int COMMENT "独立单元商品类别备注",
    group_manage_type varchar COMMENT "商品集团管理类型",
    
    -- 数量指标
    goods_qty decimal(20, 6) COMMENT "商品总数量",
    
    -- 金额指标
    settle_amount decimal(18,4) COMMENT "结算总金额",
    notax_amount decimal(18,4) COMMENT "不含税总金额",
    cost_amount decimal(18,4) COMMENT "成本总金额",
    
    -- 利润指标
    batch_gross_profit decimal(18,4) COMMENT "批次总毛利",
    notax_gross_profit decimal(18,4) COMMENT "不含税总毛利",
    gross_profit_rate decimal(18,4) COMMENT "平均不含税毛利率"
)
UNIQUE KEY(stat_date, goodsid, entryid, customid, is_jicai_zhongxuan) 
DISTRIBUTED BY HASH(stat_date, goodsid) 
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 插入数据：按日期、商品、独立单元维度聚合
INSERT INTO dws.wholesale_settle_goods_d (
    stat_date,
    goodsid,
    entryid,
    entry_name,
    province_name,
    city_name,
    area_name,
    caiwu_level1,
    caiwu_level2,
    customid,
    customer_name,
    customertype_name,
    customertype_group,
    customer_financeclass_name,
    is_jicai_zhongxuan,
    nianbao_type,
    qixie_class,
    qixie_brandtype,
    leibiebeizhu,
    group_manage_type,
    goods_qty,
    settle_amount,
    notax_amount,
    cost_amount,
    batch_gross_profit,
    notax_gross_profit,
    gross_profit_rate
)
SELECT
    DATE(confirm_date) AS stat_date,
    goodsid,
    entryid,
    entry_name,
    province_name,
    city_name,
    area_name,
    caiwu_level1,
    caiwu_level2,
    customid,
    customer_name,
    customertype_name,
    customertype_group,
    customer_financeclass_name,
    is_jicai_zhongxuan,
    IFNULL(nianbao_type, 'UNKNOWN') as nianbao_type,
    IFNULL(qixie_class, 'UNKNOWN') as qixie_class,
    IFNULL(qixie_brandtype, 'UNKNOWN') as qixie_brandtype,
    IFNULL(leibiebeizhu, -1) as leibiebeizhu,
    group_manage_type,
    SUM(goods_qty) AS goods_qty,
    SUM(settle_amount) AS settle_amount,
    SUM(notax_amount) AS notax_amount,
    SUM(cost_amount) AS cost_amount,
    SUM(batch_gross_profit) AS batch_gross_profit,
    SUM(notax_gross_profit) AS notax_gross_profit,
    CASE 
        WHEN SUM(notax_amount) = 0 THEN CAST(0 as decimal(18,4))
        ELSE ROUND(SUM(notax_gross_profit) / SUM(notax_amount), 4)
    END AS gross_profit_rate
FROM 
    dwd.wholesale_settle_dtl
WHERE 
    use_status = '正式'
GROUP BY 
    DATE(confirm_date),
    goodsid,
    entryid,
    entry_name,
    province_name,
    city_name,
    area_name,
    caiwu_level1,
    caiwu_level2,
    customid,
    customer_name,
    customertype_name,
    customertype_group,
    customer_financeclass_name,
    is_jicai_zhongxuan,
    nianbao_type,
    qixie_class,
    qixie_brandtype,
    leibiebeizhu,
    group_manage_type;

-- Create new indexes
CREATE INDEX IF NOT EXISTS idx_stat_date ON dws.wholesale_order_settle_goods_d (stat_date);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dws.wholesale_order_settle_goods_d (goodsid);
CREATE INDEX IF NOT EXISTS idx_entryid ON dws.wholesale_order_settle_goods_d (entryid);
CREATE INDEX IF NOT EXISTS idx_entry_name ON dws.wholesale_order_settle_goods_d (entry_name);
CREATE INDEX IF NOT EXISTS idx_province_name ON dws.wholesale_order_settle_goods_d (province_name);
CREATE INDEX IF NOT EXISTS idx_city_name ON dws.wholesale_order_settle_goods_d (city_name);
CREATE INDEX IF NOT EXISTS idx_area_name ON dws.wholesale_order_settle_goods_d (area_name);
CREATE INDEX IF NOT EXISTS idx_caiwu_level1 ON dws.wholesale_order_settle_goods_d (caiwu_level1);
CREATE INDEX IF NOT EXISTS idx_caiwu_level2 ON dws.wholesale_order_settle_goods_d (caiwu_level2);
CREATE INDEX IF NOT EXISTS idx_customid ON dws.wholesale_order_settle_goods_d (customid);
CREATE INDEX IF NOT EXISTS idx_customer_name ON dws.wholesale_order_settle_goods_d (customer_name);
CREATE INDEX IF NOT EXISTS idx_customertype_name ON dws.wholesale_order_settle_goods_d (customertype_name);
CREATE INDEX IF NOT EXISTS idx_customertype_group ON dws.wholesale_order_settle_goods_d (customertype_group);
CREATE INDEX IF NOT EXISTS idx_customer_financeclass_name ON dws.wholesale_order_settle_goods_d (customer_financeclass_name);
CREATE INDEX IF NOT EXISTS idx_is_jicai_zhongxuan ON dws.wholesale_order_settle_goods_d (is_jicai_zhongxuan);
CREATE INDEX IF NOT EXISTS idx_nianbao_type ON dws.wholesale_order_settle_goods_d (nianbao_type);
CREATE INDEX IF NOT EXISTS idx_qixie_class ON dws.wholesale_order_settle_goods_d (qixie_class);
CREATE INDEX IF NOT EXISTS idx_qixie_brandtype ON dws.wholesale_order_settle_goods_d (qixie_brandtype);
CREATE INDEX IF NOT EXISTS idx_leibiebeizhu ON dws.wholesale_order_settle_goods_d (leibiebeizhu);
CREATE INDEX IF NOT EXISTS idx_group_manage_type ON dws.wholesale_order_settle_goods_d (group_manage_type);