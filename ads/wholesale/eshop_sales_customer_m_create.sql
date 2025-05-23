-- 删除已存在的表，确保创建新表时不会有冲突
DROP TABLE IF EXISTS ads.eshop_sales_customer_m;

CREATE TABLE ads.eshop_sales_customer_m (
    -- 维度信息
    stat_yearmonth DATE COMMENT "业务年月",
    entryid bigint COMMENT "独立单元ID",
    customid bigint COMMENT "客户ID",

    -- 组织信息
    entry_name varchar COMMENT "独立单元名称",
    city_name varchar COMMENT "城市名称",
    city_order int COMMENT "城市顺序",

    -- 客户信息
    customer_name varchar COMMENT "客户名称",

    -- 销售总单信息
    order_count int COMMENT "销售总单数量",
    sales_amount decimal(18,4) COMMENT "销售总金额",

    -- 可转化为b2b的手工订单信息
    potential_b2b_order_count int COMMENT "可转化为b2b的订单数量",
    potential_b2b_sales_amount decimal(18,4) COMMENT "可转化为b2b的订单金额",

    -- b2b订单信息
    b2b_order_count int COMMENT "b2b订单数量",
    b2b_sales_amount decimal(18,4) COMMENT "b2b订单金额",

    -- b2b自主下单订单信息
    b2b_self_initiated_order_count int COMMENT "b2b自主下单订单数量",
    b2b_self_initiated_sales_amount decimal(18,4) COMMENT "b2b自主下单订单金额",

    -- 可转化为b2b的手工订单信息（转化率）
    potential_b2b_order_count_rate decimal(18,4) COMMENT "可转化为b2b的订单数量转化率",
    potential_b2b_sales_amount_rate decimal(18,4) COMMENT "可转化为b2b的订单金额转化率",

    -- b2b订单情况/可转化为b2b的手工订单信息（转化率）
    b2b_order_count_rate decimal(18,4) COMMENT "b2b订单数量转化率",
    b2b_sales_amount_rate decimal(18,4) COMMENT "b2b订单金额转化率",

    -- b2b自主下单订单/可转化为b2b的手工订单信息（转化率）
    b2b_self_initiated_order_count_rate decimal(18,4) COMMENT "b2b自主下单订单数量转化率",
    b2b_self_initiated_sales_amount_rate decimal(18,4) COMMENT "b2b自主下单订单金额转化率",

    -- b2b自主下单订单/b2b订单情况（占比）
    b2b_self_initiated_order_count_proportion decimal(18,4) COMMENT "b2b自主下单订单数量占比",
    b2b_self_initiated_sales_amount_proportion decimal(18,4) COMMENT "b2b自主下单订单金额占比"
)
UNIQUE KEY(stat_yearmonth, entryid, customid) 
DISTRIBUTED BY HASH(stat_yearmonth, entryid, customid) 
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 插入历史每月数据到月度汇总表
INSERT INTO ads.eshop_sales_customer_m (
    stat_yearmonth,
    entryid,
    customid,
    entry_name,
    city_name,
    city_order,
    customer_name,
    order_count,
    sales_amount,
    potential_b2b_order_count,
    potential_b2b_sales_amount,
    b2b_order_count,
    b2b_sales_amount,
    b2b_self_initiated_order_count,
    b2b_self_initiated_sales_amount,
    potential_b2b_order_count_rate,
    potential_b2b_sales_amount_rate,
    b2b_order_count_rate,
    b2b_sales_amount_rate,
    b2b_self_initiated_order_count_rate,
    b2b_self_initiated_sales_amount_rate,
    b2b_self_initiated_order_count_proportion,
    b2b_self_initiated_sales_amount_proportion
)
WITH 
-- 当前月数据
current_month_data AS (
    SELECT 
        date_trunc(stat_date, 'month') AS stat_yearmonth,
        entryid,
        customid,
        MAX(entry_name) AS entry_name,
        MAX(city_name) AS city_name,
        MAX(city_order) AS city_order,
        MAX(customer_name) AS customer_name,
        SUM(order_count) AS order_count,
        SUM(sales_amount) AS sales_amount,
        SUM(potential_b2b_order_count) AS potential_b2b_order_count,
        SUM(potential_b2b_sales_amount) AS potential_b2b_sales_amount,
        SUM(b2b_order_count) AS b2b_order_count,
        SUM(b2b_sales_amount) AS b2b_sales_amount,
        SUM(b2b_self_initiated_order_count) AS b2b_self_initiated_order_count,
        SUM(b2b_self_initiated_sales_amount) AS b2b_self_initiated_sales_amount
    FROM dws.eshop_sales_customer_d
    GROUP BY date_trunc(stat_date, 'month'), entryid, customid
)
-- 计算转化率和占比
SELECT
    cm.stat_yearmonth,
    cm.entryid,
    cm.customid,
    cm.entry_name,
    cm.city_name,
    cm.city_order,
    cm.customer_name,

    -- 销售总单信息
    cm.order_count,
    cm.sales_amount,

    -- 可转化为b2b的手工订单信息
    cm.potential_b2b_order_count,
    cm.potential_b2b_sales_amount,
    
    -- b2b订单信息
    cm.b2b_order_count,
    cm.b2b_sales_amount,
    
    -- b2b自主下单订单信息
    cm.b2b_self_initiated_order_count,
    cm.b2b_self_initiated_sales_amount,

    -- 可转化为b2b的手工订单信/销售总单信息（转化率）
    CASE
        WHEN IFNULL(cm.order_count, 0) = 0 THEN 0
        ELSE ROUND(cm.potential_b2b_order_count / cm.order_count, 4)
    END AS potential_b2b_order_count_rate,
    CASE
        WHEN IFNULL(cm.sales_amount, 0) = 0 THEN 0
        ELSE ROUND(cm.potential_b2b_sales_amount / cm.sales_amount, 4)
    END AS potential_b2b_sales_amount_rate,
    
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