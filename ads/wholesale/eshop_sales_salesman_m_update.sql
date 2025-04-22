INSERT INTO ads.eshop_sales_salesman_m (
    stat_yearmonth,
    entryid,
    salerid,
    entry_name,
    city_name,
    city_order,
    saler_name,
    potential_b2b_order_count,
    potential_b2b_sales_amount,
    b2b_order_count,
    b2b_sales_amount,
    b2b_self_initiated_order_count,
    b2b_self_initiated_sales_amount,
    b2b_order_count_rate,
    b2b_sales_amount_rate,
    b2b_self_initiated_order_count_rate,
    b2b_self_initiated_sales_amount_rate,
    b2b_self_initiated_order_count_proportion,
    b2b_self_initiated_sales_amount_proportion
)
WITH 
-- 当前月数据（包括本月和上个月）
current_month_data AS (
    SELECT 
        DATE_TRUNC(stat_date, 'month') AS stat_yearmonth,
        entryid,
        salerid,
        MAX(entry_name) AS entry_name,
        MAX(city_name) AS city_name,
        MAX(city_order) AS city_order,
        MAX(saler_name) AS saler_name,
        SUM(potential_b2b_order_count) AS potential_b2b_order_count,
        SUM(potential_b2b_sales_amount) AS potential_b2b_sales_amount,
        SUM(b2b_order_count) AS b2b_order_count,
        SUM(b2b_sales_amount) AS b2b_sales_amount,
        SUM(b2b_self_initiated_order_count) AS b2b_self_initiated_order_count,
        SUM(b2b_self_initiated_sales_amount) AS b2b_self_initiated_sales_amount
    FROM dws.eshop_sales_salesman_d
    WHERE DATE_FORMAT(stat_date, '%Y%m') IN (
        DATE_FORMAT(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), '%Y%m'),
        DATE_FORMAT(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 1 MONTH), '%Y%m')
    )
    GROUP BY DATE_TRUNC(stat_date, 'month'), entryid, salerid
)
-- 计算各种转化率和占比
SELECT
    cm.stat_yearmonth,
    cm.entryid,
    cm.salerid,
    cm.entry_name,
    cm.city_name,
    cm.city_order,
    cm.saler_name,
    
    -- 可转化为b2b的手工订单信息
    cm.potential_b2b_order_count,
    cm.potential_b2b_sales_amount,
    
    -- b2b订单信息
    cm.b2b_order_count,
    cm.b2b_sales_amount,
    
    -- b2b自主下单订单信息
    cm.b2b_self_initiated_order_count,
    cm.b2b_self_initiated_sales_amount,
    
    -- b2b订单情况/可转化为b2b的手工订单信息（转化率）
    CASE 
        WHEN IFNULL(cm.potential_b2b_order_count, 0) = 0 THEN 0
        ELSE ROUND(cm.b2b_order_count / cm.potential_b2b_order_count, 4)
    END AS b2b_order_count_rate,
    CASE 
        WHEN IFNULL(cm.potential_b2b_sales_amount, 0) = 0 THEN 0
        ELSE ROUND(cm.b2b_sales_amount / cm.potential_b2b_sales_amount, 4)
    END AS b2b_sales_amount_rate,
    
    -- b2b自主下单订单/可转化为b2b的手工订单信息（转化率）
    CASE 
        WHEN IFNULL(cm.potential_b2b_order_count, 0) = 0 THEN 0
        ELSE ROUND(cm.b2b_self_initiated_order_count / cm.potential_b2b_order_count, 4)
    END AS b2b_self_initiated_order_count_rate,
    CASE 
        WHEN IFNULL(cm.potential_b2b_sales_amount, 0) = 0 THEN 0
        ELSE ROUND(cm.b2b_self_initiated_sales_amount / cm.potential_b2b_sales_amount, 4)
    END AS b2b_self_initiated_sales_amount_rate,
    
    -- b2b自主下单订单/b2b订单情况（占比）
    CASE 
        WHEN IFNULL(cm.b2b_order_count, 0) = 0 THEN 0
        ELSE ROUND(cm.b2b_self_initiated_order_count / cm.b2b_order_count, 4)
    END AS b2b_self_initiated_order_count_proportion,
    CASE 
        WHEN IFNULL(cm.b2b_sales_amount, 0) = 0 THEN 0
        ELSE ROUND(cm.b2b_self_initiated_sales_amount / cm.b2b_sales_amount, 4)
    END AS b2b_self_initiated_sales_amount_proportion
FROM current_month_data cm;