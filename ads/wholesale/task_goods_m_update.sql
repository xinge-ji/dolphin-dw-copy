INSERT INTO ads.wholesale_task_goods_m (
    stat_yearmonth,
    entryid,
    docid,
    goodsid,
    task_name,
    entry_name,
    goods_name,
    area_name,
    sales_amount,
    new_customer_sales_amount,
    new_customer_count,
    retention_customer_count
)
WITH 
-- 月度销售数据（仅包含当前月和上个月）
monthly_sales AS (
    SELECT
        DATE_TRUNC(sg.stat_date, 'month') AS stat_yearmonth,
        sg.entryid,
        ts.docid,
        sg.goodsid,
        IFNULL(SUM(sg.sales_amount), 0) AS sales_amount
    FROM
        dws.wholesale_sales_goods_d sg
    JOIN
        dim.wholesale_task_set ts 
        ON sg.entryid = ts.entryid 
        AND sg.customid = ts.customid
    JOIN
        dim.goods_set gs 
        ON ts.goods_set_id = gs.setid 
        AND sg.goodsid = gs.goodsid
    WHERE 
        DATE_TRUNC(sg.stat_date, 'month') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 1 MONTH)
        AND DATE_TRUNC(sg.stat_date, 'month') <= DATE_TRUNC(CURRENT_DATE(), 'month')
    GROUP BY
        DATE_TRUNC(sg.stat_date, 'month'),
        sg.entryid,
        ts.docid,
        sg.goodsid
),

-- 客户购买记录（仅包含当前月和上个月）
customer_purchase AS (
    SELECT
        DATE_TRUNC(sg.stat_date, 'month') AS stat_yearmonth,
        sg.entryid,
        ts.docid,
        sg.goodsid,
        sg.customid,
        IFNULL(COUNT(sg.sales_amount>0), 0) AS purchase_count,
        IFNULL(SUM(sg.sales_amount), 0) AS sales_amount
    FROM
        dws.wholesale_sales_goods_d sg
    JOIN
        dim.wholesale_task_set ts 
        ON sg.entryid = ts.entryid 
        AND sg.customid = ts.customid
    JOIN
        dim.goods_set gs 
        ON ts.goods_set_id = gs.setid 
        AND sg.goodsid = gs.goodsid
    WHERE 
        DATE_TRUNC(sg.stat_date, 'month') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 1 MONTH)
        AND DATE_TRUNC(sg.stat_date, 'month') <= DATE_TRUNC(CURRENT_DATE(), 'month')
    GROUP BY
        DATE_TRUNC(sg.stat_date, 'month'),
        sg.entryid,
        ts.docid,
        sg.goodsid,
        sg.customid
),

-- 历史购买记录（用于判断是否为新客户）
historical_purchase AS (
    SELECT
        cp.entryid,
        cp.docid,
        cp.goodsid,
        cp.customid,
        MIN(cp.stat_yearmonth) AS first_purchase_month
    FROM
        customer_purchase cp
    GROUP BY
        cp.entryid,
        cp.docid,
        cp.goodsid,
        cp.customid
),

-- 客户统计
customer_stats AS (
    SELECT
        cp.stat_yearmonth,
        cp.entryid,
        cp.docid,
        cp.goodsid,
        -- 新客户数：本月是首次购买该商品的客户数
        COUNT(DISTINCT CASE 
            WHEN hp.first_purchase_month = cp.stat_yearmonth 
            THEN cp.customid 
        END) AS new_customer_count,
        -- 新客户销售额：本月首次购买该商品的客户销售额
        SUM(CASE 
            WHEN hp.first_purchase_month = cp.stat_yearmonth 
            THEN cp.sales_amount
            ELSE 0
        END) AS new_customer_sales_amount,
        -- 留存客户数：本月首次购买且购买该商品超过1次的客户数
        COUNT(DISTINCT CASE 
            WHEN hp.first_purchase_month = cp.stat_yearmonth AND cp.purchase_count > 1 
            THEN cp.customid 
        END) AS retention_customer_count
    FROM
        customer_purchase cp
    JOIN
        historical_purchase hp
        ON cp.entryid = hp.entryid
        AND cp.docid = hp.docid
        AND cp.goodsid = hp.goodsid
        AND cp.customid = hp.customid
    GROUP BY
        cp.stat_yearmonth,
        cp.entryid,
        cp.docid,
        cp.goodsid
)

-- 最终结果
SELECT
    ms.stat_yearmonth,
    ms.entryid,
    ms.docid,
    ms.goodsid,
    ts.task_name,
    e.entry_name,
    g.goods_name,
    e.area_name,
    ms.sales_amount,
    COALESCE(cs.new_customer_sales_amount, 0) AS new_customer_sales_amount,
    COALESCE(cs.new_customer_count, 0) AS new_customer_count,
    COALESCE(cs.retention_customer_count, 0) AS retention_customer_count
FROM
    monthly_sales ms
JOIN
    dim.wholesale_task_set ts 
    ON ms.entryid = ts.entryid 
    AND ms.docid = ts.docid
JOIN
    dim.entry e 
    ON ms.entryid = e.entryid
    AND e.is_active = 1
JOIN
    dim.goods g 
    ON ms.goodsid = g.goodsid
    AND g.is_active = 1
LEFT JOIN
    customer_stats cs 
    ON ms.stat_yearmonth = cs.stat_yearmonth 
    AND ms.entryid = cs.entryid 
    AND ms.docid = cs.docid 
    AND ms.goodsid = cs.goodsid;