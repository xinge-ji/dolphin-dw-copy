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
-- 月度销售数据
monthly_sales AS (
    SELECT
        DATE_TRUNC(sg.stat_date, 'month') AS stat_yearmonth,
        sg.entryid,
        sg.entry_name,
        sg.area_name,
        ts.docid,
        ts.task_name,
        sg.goodsid,
        sg.goods_name,
        SUM(sg.sales_amount) AS sales_amount
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
        sg.entry_name,
        sg.area_name,
        ts.docid,
        ts.task_name,
        sg.goodsid,
        sg.goods_name
),

-- 客户购买记录
customer_purchase AS (
    SELECT
        DATE_TRUNC(sg.stat_date, 'month') AS stat_yearmonth,
        sg.entryid,
        sg.entry_name,
        sg.area_name,
        sg.customid,
        ts.docid,
        ts.task_name,
        sg.goodsid,
        sg.goods_name,
        IFNULL(SUM(CASE WHEN sg.sales_amount > 0 THEN 1 ELSE 0 END), 0) AS purchase_count,
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
        sg.entry_name,
        sg.area_name,
        sg.customid,
        ts.docid,
        ts.task_name,
        sg.goodsid,
        sg.goods_name
),

-- 历史购买记录
historical_purchase AS (
    SELECT
        cp.entryid,
        cp.entry_name,
        cp.area_name,
        cp.customid,
        cp.docid,
        cp.task_name,
        cp.goodsid,
        cp.goods_name,
        MIN(cp.stat_yearmonth) AS first_purchase_month
    FROM
        customer_purchase cp
    GROUP BY
        cp.entryid,
        cp.entry_name,
        cp.area_name,
        cp.customid,
        cp.docid,
        cp.task_name,
        cp.goodsid,
        cp.goods_name
),

-- 客户统计
customer_stats AS (
    SELECT
        cp.stat_yearmonth,
        cp.entryid,
        cp.entry_name,
        cp.area_name,
        cp.customid,
        cp.docid,
        cp.task_name,
        cp.goodsid,
        cp.goods_name,
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
        cp.entry_name,
        cp.area_name,
        cp.customid,
        cp.docid,
        cp.task_name,
        cp.goodsid,
        cp.goods_name
)

-- 最终结果
SELECT
    ms.stat_yearmonth,
    ms.entryid,
    ms.docid,
    ms.goodsid,
    ms.task_name,
    ms.entry_name,
    ms.goods_name,
    ms.area_name,
    ms.sales_amount,
    COALESCE(cs.new_customer_sales_amount, 0) AS new_customer_sales_amount,
    COALESCE(cs.new_customer_count, 0) AS new_customer_count,
    COALESCE(cs.retention_customer_count, 0) AS retention_customer_count
FROM
    monthly_sales ms
LEFT JOIN
    customer_stats cs 
    ON ms.stat_yearmonth = cs.stat_yearmonth 
    AND ms.entryid = cs.entryid 
    AND ms.docid = cs.docid 
    AND ms.goodsid = cs.goodsid
    AND ms.entry_name = cs.entry_name
    AND ms.task_name = cs.task_name
    AND ms.goods_name = cs.goods_name
    AND ms.area_name = cs.area_name;