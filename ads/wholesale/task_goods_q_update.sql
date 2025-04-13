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
        DATE_FORMAT(DATE_TRUNC(sg.stat_date, 'quarter'), '%Y-%m-01') AS stat_yearquarter,
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
        DATE_TRUNC(sg.stat_date, 'quarter') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'quarter'), INTERVAL 1 QUARTER)
        AND DATE_TRUNC(sg.stat_date, 'quarter') <= DATE_TRUNC(CURRENT_DATE(), 'quarter')
    GROUP BY
        DATE_FORMAT(DATE_TRUNC(sg.stat_date, 'quarter'), '%Y-%m-01'),
        sg.entryid,
        ts.docid,
        sg.goodsid
),

-- 客户购买记录（仅包含当前季度和上季度）
customer_purchase AS (
    SELECT
        DATE_FORMAT(DATE_TRUNC(sg.stat_date, 'quarter'), '%Y-%m-01') AS stat_yearquarter,
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
        DATE_TRUNC(sg.stat_date, 'quarter') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'quarter'), INTERVAL 1 QUARTER)
        AND DATE_TRUNC(sg.stat_date, 'quarter') <= DATE_TRUNC(CURRENT_DATE(), 'quarter')
    GROUP BY
        DATE_FORMAT(DATE_TRUNC(sg.stat_date, 'quarter'), '%Y-%m-01'),
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
        MIN(cp.stat_yearquarter) AS first_purchase_quarter
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
        cp.stat_yearquarter,
        cp.entryid,
        cp.docid,
        cp.goodsid,
        -- 新客户数：本季度是首次购买该商品的客户数
        COUNT(DISTINCT CASE 
            WHEN hp.first_purchase_quarter = cp.stat_yearquarter 
            THEN cp.customid 
        END) AS new_customer_count,
        -- 新客户销售额：本季度首次购买该商品的客户销售额
        SUM(CASE 
            WHEN hp.first_purchase_quarter = cp.stat_yearquarter 
            THEN cp.sales_amount
            ELSE 0
        END) AS new_customer_sales_amount,
        -- 留存客户数：本季度首次购买且购买该商品超过1次的客户数
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
        cp.docid,
        cp.goodsid
)

-- 最终结果
SELECT
    qs.stat_yearquarter,
    qs.entryid,
    qs.docid,
    qs.goodsid,
    ts.task_name,
    e.entry_name,
    g.goods_name,
    e.area_name,
    qs.sales_amount,
    COALESCE(cs.new_customer_sales_amount, 0) AS new_customer_sales_amount,
    COALESCE(cs.new_customer_count, 0) AS new_customer_count,
    COALESCE(cs.retention_customer_count, 0) AS retention_customer_count
FROM
    quarterly_sales qs
JOIN
    dim.wholesale_task_set ts 
    ON qs.entryid = ts.entryid 
    AND qs.docid = ts.docid
JOIN
    dim.entry e 
    ON qs.entryid = e.entryid
    AND e.is_active = 1
JOIN
    dim.goods g 
    ON qs.goodsid = g.goodsid
    AND g.is_active = 1
LEFT JOIN
    customer_stats cs 
    ON qs.stat_yearquarter = cs.stat_yearquarter 
    AND qs.entryid = cs.entryid 
    AND qs.docid = cs.docid 
    AND qs.goodsid = cs.goodsid;