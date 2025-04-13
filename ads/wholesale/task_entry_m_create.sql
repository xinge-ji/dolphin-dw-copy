DROP TABLE IF EXISTS ads.wholesale_task_entry_m;
CREATE TABLE ads.wholesale_task_entry_m (
    -- 颗粒度
    stat_yearmonth DATE COMMENT "业务年月",
    entryid bigint COMMENT '独立单元ID',
    docid bigint COMMENT '项目ID',
    customertype_task varchar comment '分销项目客户类型:等级机构/基层/终端/企业',

    -- 基础信息
    task_name varchar(255) COMMENT '项目名称',
    entry_name varchar(255) COMMENT '独立单元名称',

    -- 维度
    area_name varchar(255) COMMENT '区域名称',
    
    -- 指标
    goods_count int COMMENT '项目关联商品数量',
    sales_amount decimal(20,2) COMMENT '销售额',
    customer_count int COMMENT '本月开单客户数',
    repurchase_sales_amount decimal(20,2) COMMENT '复购销售额',
    repurchase_customer_count int COMMENT '复购客户数',
    prev_1m_repurchase_customer_count int COMMENT '本月及上月都有购买的客户数',
    prev_2m_repurchase_customer_count int COMMENT '本月及前两个月内都有购买的客户数',
    prev_3m_repurchase_customer_count int COMMENT '本月及前三个月内都有购买的客户数',
    cumulate_customer_count int COMMENT '累计客户数',
    churn_customer_count int COMMENT '本月流失客户数',
    new_customer_count int COMMENT '本月新增客户数',
    avg_customer_lifecycle_months int COMMENT '平均本月开单客户生命周期（月）'
) UNIQUE KEY (stat_yearmonth, entryid, docid, customertype_task) DISTRIBUTED BY HASH (docid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
);

-- 插入数据到批发任务独立单元月度汇总表
INSERT INTO ads.wholesale_task_entry_m (
    stat_yearmonth,
    entryid,
    docid,
    customertype_task,
    task_name,
    entry_name,
    area_name,
    goods_count,
    sales_amount,
    customer_count,
    repurchase_sales_amount,
    repurchase_customer_count,
    prev_1m_repurchase_customer_count,
    prev_2m_repurchase_customer_count,
    prev_3m_repurchase_customer_count,
    cumulate_customer_count,
    churn_customer_count,
    new_customer_count,
    avg_customer_lifecycle_months
)
WITH 
-- 项目基础信息
task_base AS (
    SELECT
        ts.docid,
        ts.task_name,
        ts.entryid,
        ts.goods_count,
        e.entry_name,
        e.area_name,
        c.customertype_task
    FROM
        dim.wholesale_task_set ts
    JOIN
        dim.entry e ON ts.entryid = e.entryid
    JOIN
        dim.customer c ON ts.customid = c.customid
    GROUP BY
        ts.docid,
        ts.task_name,
        ts.entryid,
        ts.goods_count,
        e.entry_name,
        e.area_name,
        c.customertype_task
),

-- 月度销售数据
monthly_sales AS (
    SELECT
        DATE_TRUNC(sg.stat_date, 'month') AS stat_yearmonth,
        sg.entryid,
        ts.docid,
        sg.customertype_task,
        SUM(sg.sales_amount) AS sales_amount,
        COUNT(DISTINCT sg.customid) AS customer_count
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
    GROUP BY
        DATE_TRUNC(sg.stat_date, 'month'),
        sg.entryid,
        ts.docid,
        sg.customertype_task
),

-- 历史购买记录
customer_purchase_history AS (
    SELECT
        sg.customid,
        sg.entryid,
        ts.docid,
        MIN(sg.stat_date) AS first_purchase_date,
        MAX(sg.stat_date) AS last_purchase_date
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
    GROUP BY
        sg.customid,
        sg.entryid,
        ts.docid
),

-- 按月份统计客户购买情况
monthly_customer_purchase AS (
    SELECT
        ts.customid,
        ts.entryid,
        ts.docid,
        DATE_TRUNC(sg.stat_date, 'month') AS purchase_month
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
    GROUP BY
        ts.customid,
        ts.entryid,
        ts.docid,
        DATE_TRUNC(sg.stat_date, 'month')
),

-- 复购客户数据
repurchase_data AS (
    SELECT
        ms.stat_yearmonth,
        ms.entryid,
        ms.docid,
        ms.customertype_task,
        -- 复购销售额
        SUM(CASE 
            WHEN cph.customid IS NOT NULL AND cph.first_purchase_date < DATE_TRUNC(sg.stat_date, 'month')
            THEN sg.sales_amount - COALESCE(sg.return_amount, 0)
            ELSE 0 
        END) AS repurchase_sales_amount,
        -- 复购客户数
        COUNT(DISTINCT CASE 
            WHEN cph.customid IS NOT NULL AND cph.first_purchase_date < DATE_TRUNC(sg.stat_date, 'month')
            THEN sg.customid 
        END) AS repurchase_customer_count,
        -- 本月及上月都有购买的客户数
        COUNT(DISTINCT CASE 
            WHEN mcp_prev1.customid IS NOT NULL
            THEN sg.customid 
        END) AS prev_1m_repurchase_customer_count,
        -- 本月及前两个月内都有购买的客户数
        COUNT(DISTINCT CASE 
            WHEN mcp_prev2.customid IS NOT NULL
            THEN sg.customid 
        END) AS prev_2m_repurchase_customer_count,
        -- 本月及前三个月内都有购买的客户数
        COUNT(DISTINCT CASE 
            WHEN mcp_prev3.customid IS NOT NULL
            THEN sg.customid 
        END) AS prev_3m_repurchase_customer_count
    FROM
        monthly_sales ms
    JOIN
        dws.wholesale_sales_goods_d sg 
        ON DATE_TRUNC(sg.stat_date, 'month') = ms.stat_yearmonth
        AND sg.entryid = ms.entryid
        AND sg.customertype_task = ms.customertype_task
    JOIN
        dim.wholesale_task_set ts 
        ON sg.entryid = ts.entryid 
        AND sg.customid = ts.customid
        AND ts.docid = ms.docid
    JOIN
        dim.goods_set gs 
        ON ts.goods_set_id = gs.setid 
        AND sg.goodsid = gs.goodsid
    LEFT JOIN
        customer_purchase_history cph
        ON sg.customid = cph.customid
        AND sg.entryid = cph.entryid
        AND ms.docid = cph.docid
    LEFT JOIN
        monthly_customer_purchase mcp_prev1
        ON sg.customid = mcp_prev1.customid
        AND sg.entryid = mcp_prev1.entryid
        AND ms.docid = mcp_prev1.docid
        AND mcp_prev1.purchase_month = DATE_SUB(ms.stat_yearmonth, INTERVAL 1 MONTH)
    LEFT JOIN
        monthly_customer_purchase mcp_prev2
        ON sg.customid = mcp_prev2.customid
        AND sg.entryid = mcp_prev2.entryid
        AND ms.docid = mcp_prev2.docid
        AND mcp_prev2.purchase_month BETWEEN DATE_SUB(ms.stat_yearmonth, INTERVAL 2 MONTH) 
                                      AND DATE_SUB(ms.stat_yearmonth, INTERVAL 1 MONTH)
    LEFT JOIN
        monthly_customer_purchase mcp_prev3
        ON sg.customid = mcp_prev3.customid
        AND sg.entryid = mcp_prev3.entryid
        AND ms.docid = mcp_prev3.docid
        AND mcp_prev3.purchase_month BETWEEN DATE_SUB(ms.stat_yearmonth, INTERVAL 3 MONTH) 
                                      AND DATE_SUB(ms.stat_yearmonth, INTERVAL 1 MONTH)
    GROUP BY
        ms.stat_yearmonth,
        ms.entryid,
        ms.docid,
        ms.customertype_task
),

-- 客户生命周期数据
customer_lifecycle AS (
    SELECT
        ms.stat_yearmonth,
        ms.entryid,
        ms.docid,
        ms.customertype_task,
        -- 累计客户数（截至当前月份的累计客户数）
        COUNT(DISTINCT CASE 
            WHEN cph.first_purchase_date <= LAST_DAY(ms.stat_yearmonth)
            THEN cph.customid 
        END) AS cumulate_customer_count,
        -- 本月流失客户数（本月未购买的客户数量）
        COUNT(DISTINCT CASE 
            WHEN cph.first_purchase_date < ms.stat_yearmonth
                AND mcp_curr.customid IS NULL
            THEN cph.customid 
        END) AS churn_customer_count,
        -- 本月新增客户数
        COUNT(DISTINCT CASE 
            WHEN DATE_TRUNC(cph.first_purchase_date, 'month') = ms.stat_yearmonth
            THEN cph.customid 
        END) AS new_customer_count,
        -- 平均本月开单客户生命周期（月）
        AVG(CASE 
            WHEN mcp_curr.customid IS NOT NULL
            THEN TIMESTAMPDIFF(MONTH, cph.first_purchase_date, cph.last_purchase_date) + 1
        END) AS avg_customer_lifecycle_months
    FROM
        monthly_sales ms
    JOIN
        customer_purchase_history cph
        ON ms.entryid = cph.entryid
        AND ms.docid = cph.docid
    LEFT JOIN
        monthly_customer_purchase mcp_curr
        ON cph.customid = mcp_curr.customid
        AND cph.entryid = mcp_curr.entryid
        AND cph.docid = mcp_curr.docid
        AND mcp_curr.purchase_month = ms.stat_yearmonth
    GROUP BY
        ms.stat_yearmonth,
        ms.entryid,
        ms.docid,
        ms.customertype_task
)

-- 最终结果
SELECT
    ms.stat_yearmonth,
    ms.entryid,
    ms.docid,
    ms.customertype_task,
    tb.task_name,
    tb.entry_name,
    tb.area_name,
    tb.goods_count,
    ms.sales_amount,
    ms.customer_count,
    COALESCE(rd.repurchase_sales_amount, 0) AS repurchase_sales_amount,
    COALESCE(rd.repurchase_customer_count, 0) AS repurchase_customer_count,
    COALESCE(rd.prev_1m_repurchase_customer_count, 0) AS prev_1m_repurchase_customer_count,
    COALESCE(rd.prev_2m_repurchase_customer_count, 0) AS prev_2m_repurchase_customer_count,
    COALESCE(rd.prev_3m_repurchase_customer_count, 0) AS prev_3m_repurchase_customer_count,
    COALESCE(cl.cumulate_customer_count, 0) AS cumulate_customer_count,
    COALESCE(cl.churn_customer_count, 0) AS churn_customer_count,
    COALESCE(cl.new_customer_count, 0) AS new_customer_count,
    COALESCE(cl.avg_customer_lifecycle_months, 0) AS avg_customer_lifecycle_months
FROM
    monthly_sales ms
JOIN
    task_base tb 
    ON ms.entryid = tb.entryid 
    AND ms.docid = tb.docid 
    AND ms.customertype_task = tb.customertype_task
LEFT JOIN
    repurchase_data rd 
    ON ms.stat_yearmonth = rd.stat_yearmonth 
    AND ms.entryid = rd.entryid 
    AND ms.docid = rd.docid 
    AND ms.customertype_task = rd.customertype_task
LEFT JOIN
    customer_lifecycle cl 
    ON ms.stat_yearmonth = cl.stat_yearmonth 
    AND ms.entryid = cl.entryid 
    AND ms.docid = cl.docid 
    AND ms.customertype_task = cl.customertype_task;