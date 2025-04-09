INSERT INTO ads.eshop_order_sales_goods_m (
    stat_yearmonth,
    entryid,
    goodsid,
    entry_name,
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
-- 当前月数据（包括本月和上个月）
current_month_data AS (
    SELECT 
        date_trunc(stat_date, 'month') AS stat_yearmonth,
        entryid,
        goodsid,
        MAX(entry_name) AS entry_name,
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
    WHERE DATE_FORMAT(stat_date, '%Y%m') IN (
        DATE_FORMAT(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), '%Y%m'),
        DATE_FORMAT(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 1 MONTH), '%Y%m')
    )
    GROUP BY date_trunc(stat_date, 'month'), entryid, goodsid
)
-- 计算各种转化率和占比
SELECT
    cm.stat_yearmonth,
    cm.entryid,
    cm.goodsid,
    cm.entry_name,
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
FROM current_month_data cm
WHERE cm.entryid in (1,2,5,104,124,144,164,204,224) and cm.stat_yearmonth is not null;