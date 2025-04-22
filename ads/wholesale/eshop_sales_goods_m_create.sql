DROP TABLE IF EXISTS ads.eshop_sales_goods_m;

CREATE TABLE ads.eshop_sales_goods_m (
    -- 维度信息
    stat_yearmonth DATE COMMENT "业务年月",
    entryid bigint COMMENT "独立单元ID",
    goodsid bigint COMMENT "商品ID",

    -- 组织信息
    entry_name varchar COMMENT "独立单元名称",
    city_name varchar COMMENT "城市名称",
    city_order int COMMENT "城市顺序",

    -- 商品信息
    goods_name varchar,
    nianbao_type varchar COMMENT "商品年报类型",
    group_manage_type varchar COMMENT "商品集团管理类型",
    variety_level1_name varchar COMMENT "商品分类小类",
    variety_level2_name varchar COMMENT "商品分类中类",
    variety_level3_name varchar COMMENT "商品分类大类",

    -- 销售信息
    order_item_count int COMMENT "销售细单数量",
    sales_amount decimal(18,4) COMMENT "销售总金额",

    -- 可转化为b2b的手工订单信息
    potential_b2b_order_item_count int COMMENT "可转化为b2b的订单细单数量",
    potential_b2b_sales_amount decimal(18,4) COMMENT "可转化为b2b的订单金额",

    -- b2b订单信息
    b2b_order_item_count int COMMENT "b2b订单细单数量",
    b2b_sales_amount decimal(18,4) COMMENT "b2b订单金额",

    -- b2b自主下单订单信息
    b2b_self_initiated_order_item_count int COMMENT "b2b自主下单订单细单数量",
    b2b_self_initiated_sales_amount decimal(18,4) COMMENT "b2b自主下单订单金额"
)
UNIQUE KEY(stat_yearmonth, entryid, goodsid) 
DISTRIBUTED BY HASH(stat_yearmonth, entryid, goodsid) 
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 插入所有历史数据到商品月度汇总表
INSERT INTO ads.eshop_sales_goods_m (
    stat_yearmonth,
    entryid,
    goodsid,
    entry_name,
    city_name,
    city_order,
    goods_name,
    nianbao_type,
    group_manage_type,
    variety_level1_name,
    variety_level2_name,
    variety_level3_name,
    order_item_count,
    sales_amount,
    potential_b2b_order_item_count,
    potential_b2b_sales_amount,
    b2b_order_item_count,
    b2b_sales_amount,
    b2b_self_initiated_order_item_count,
    b2b_self_initiated_sales_amount
)
WITH 
-- 当前月数据
current_month_data AS (
    SELECT 
        date_trunc(stat_date, 'month') AS stat_yearmonth,
        entryid,
        goodsid,
        MAX(entry_name) AS entry_name,
        MAX(city_name) AS city_name,
        MAX(city_order) AS city_order,
        MAX(goods_name) AS goods_name,
        MAX(nianbao_type) AS nianbao_type,
        MAX(group_manage_type) AS group_manage_type,
        MAX(variety_level1_name) AS variety_level1_name,
        MAX(variety_level2_name) AS variety_level2_name,
        MAX(variety_level3_name) AS variety_level3_name,
        SUM(order_item_count) AS order_item_count,
        SUM(sales_amount) AS sales_amount,
        SUM(potential_b2b_order_item_count) AS potential_b2b_order_item_count,
        SUM(potential_b2b_sales_amount) AS potential_b2b_sales_amount,
        SUM(b2b_order_item_count) AS b2b_order_item_count,
        SUM(b2b_sales_amount) AS b2b_sales_amount,
        SUM(b2b_self_initiated_order_item_count) AS b2b_self_initiated_order_item_count,
        SUM(b2b_self_initiated_sales_amount) AS b2b_self_initiated_sales_amount
    FROM dws.eshop_sales_goods_d
    GROUP BY date_trunc(stat_date, 'month'), entryid, goodsid
)
-- 计算各种转化率和占比
SELECT
    cm.stat_yearmonth,
    cm.entryid,
    cm.goodsid,
    cm.entry_name,
    cm.city_name,
    cm.city_order,
    cm.goods_name,
    cm.nianbao_type,
    cm.group_manage_type,
    cm.variety_level1_name,
    cm.variety_level2_name,
    cm.variety_level3_name,
    
    -- 销售总单信息
    cm.order_item_count,
    cm.sales_amount,
    
    -- 可转化为b2b的手工订单信息
    cm.potential_b2b_order_item_count,
    cm.potential_b2b_sales_amount,
    
    -- b2b订单信息
    cm.b2b_order_item_count,
    cm.b2b_sales_amount,
    
    -- b2b自主下单订单信息
    cm.b2b_self_initiated_order_item_count,
    cm.b2b_self_initiated_sales_amount
FROM current_month_data cm;