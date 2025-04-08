DROP TABLE IF EXISTS dws.wholesale_sales_goods_d;

-- 创建批发订单结算商品日汇总表
CREATE TABLE dws.wholesale_sales_goods_d (
    -- 维度信息
    stat_date DATE COMMENT "业务日期",
    goodsid bigint COMMENT "商品ID",
    entryid bigint COMMENT "独立单元ID",
    customid bigint COMMENT "客户ID",
    goodsid bigint COMMENT "商品ID",
    
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