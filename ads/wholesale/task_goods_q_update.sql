INSERT INTO ads.wholesale_task_goods_q (
    stat_yearquarter,
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
-- 季度销售数据（仅包含当前季度和上季度）
quarterly_sales AS (
    SELECT
        DATE_TRUNC(sg.stat_date, 'quarter') AS stat_yearquarter,
        sg.entryid,
        sg.entry_name,
        sg.area_name,
        ts.docid,
        ts.task_name,
        sg.goodsid,
        sg.goods_name,
        sg.customid,
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
        DATE_TRUNC(sg.stat_date, 'quarter') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'quarter'), INTERVAL 3 MONTH)
        AND DATE_TRUNC(sg.stat_date, 'quarter') <= DATE_TRUNC(CURRENT_DATE(), 'quarter')
    GROUP BY
        DATE_TRUNC(sg.stat_date, 'quarter'),
        sg.entryid,
        sg.entry_name,
        sg.area_name,
        ts.docid,
        ts.task_name,
        sg.goodsid,
        sg.goods_name,
        sg.customid
),

-- 客户购买记录（仅包含当前季度和上季度）
customer_purchase AS (
    SELECT
        DATE_TRUNC(sg.stat_date, 'quarter') AS stat_yearquarter,
        sg.entryid,
        sg.entry_name,
        sg.area_name,
        ts.docid,
        ts.task_name,
        sg.goodsid,
        sg.goods_name,
        sg.customid,
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
        DATE_TRUNC(sg.stat_date, 'quarter') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'quarter'), INTERVAL 3 MONTH)
        AND DATE_TRUNC(sg.stat_date, 'quarter') <= DATE_TRUNC(CURRENT_DATE(), 'quarter')
    GROUP BY
        DATE_TRUNC(sg.stat_date, 'quarter'), 
        sg.entryid,
        sg.entry_name,
        sg.area_name,
        ts.docid,
        ts.task_name,
        sg.goodsid,
        sg.goods_name,
        sg.customid
),

-- 历史购买记录（季度）
historical_purchase AS (
    SELECT
        cp.entryid,
        cp.entry_name,
        cp.area_name,
        cp.docid,
        cp.task_name,
        cp.goodsid,
        cp.goods_name,
        cp.customid,
        MIN(cp.stat_yearquarter) AS first_purchase_quarter
    FROM
        customer_purchase cp
    GROUP BY
        cp.entryid,
        cp.entry_name,
        cp.area_name,
        cp.docid,
        cp.task_name,
        cp.goodsid,
        cp.goods_name,
        cp.customid
),

-- 客户统计（季度）
customer_stats AS (
    SELECT
        cp.stat_yearquarter,
        cp.entryid,
        cp.entry_name,
        cp.area_name,
        cp.docid,
        cp.task_name,
        cp.goodsid,
        cp.goods_name,
        COUNT(DISTINCT CASE 
            WHEN hp.first_purchase_quarter = cp.stat_yearquarter 
            THEN cp.customid 
        END) AS new_customer_count,
        SUM(CASE 
            WHEN hp.first_purchase_quarter = cp.stat_yearquarter 
            THEN cp.sales_amount
            ELSE 0
        END) AS new_customer_sales_amount,
        COUNT(DISTINCT CASE 
            WHEN hp.first_purchase_quarter = cp.stat_yearquarter AND cp.purchase_count > 1 
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
        cp.stat_yearquarter,
        cp.entryid,
        cp.entry_name,
        cp.area_name,
        cp.docid,
        cp.task_name,
        cp.goodsid,
        cp.goods_name
)

-- 最终结果
SELECT
    qs.stat_yearquarter,
    qs.entryid,
    qs.docid,
    qs.goodsid,
    qs.task_name,
    qs.entry_name,
    qs.goods_name,
    qs.area_name,
    qs.sales_amount,
    COALESCE(cs.new_customer_sales_amount, 0) AS new_customer_sales_amount,
    COALESCE(cs.new_customer_count, 0) AS new_customer_count,
    COALESCE(cs.retention_customer_count, 0) AS retention_customer_count
FROM
    quarterly_sales qs
LEFT JOIN
    customer_stats cs 
    ON qs.stat_yearquarter = cs.stat_yearquarter 
    AND qs.entryid = cs.entryid 
    AND qs.docid = cs.docid 
    AND qs.goodsid = cs.goodsid
    AND qs.entry_name = cs.entry_name
    AND qs.task_name = cs.task_name
    AND qs.goods_name = cs.goods_name
    AND qs.area_name = cs.area_name;