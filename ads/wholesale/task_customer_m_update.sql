INSERT INTO ads.wholesale_task_customer_m (
    stat_yearmonth,
    entryid,
    docid,
    customid,
    task_name,
    entry_name,
    customer_name,
    customertype_task,
    area_name,
    city_name,
    reputation_days,
    sales_amount,
    avg_3m_sales_amount,
    is_abnormal_sales,
    sales_threshold_deviation_rate,
    return_amount,
    is_abnormal_return,
    return_threshold_deviation_rate
)
WITH 
-- 月度销售数据（仅包含当前月和上个月）
monthly_sales AS (
    SELECT
        DATE_TRUNC(sg.stat_date, 'month') AS stat_yearmonth,
        sg.entryid,
        ts.docid,
        sg.customid,
        sg.customer_name,
        sg.customertype_task,
        sg.area_name,
        sg.city_name,
        SUM(sg.sales_amount) AS sales_amount,
        SUM(sg.return_amount) AS return_amount
    FROM
        dws.wholesale_sales_goods_d sg
    JOIN
        dim.wholesale_task_set ts 
        ON sg.entryid = ts.entryid 
        AND sg.customid = ts.customid
    WHERE 
        DATE_TRUNC(sg.stat_date, 'month') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 1 MONTH)
        AND DATE_TRUNC(sg.stat_date, 'month') <= DATE_TRUNC(CURRENT_DATE(), 'month')
    GROUP BY
        DATE_TRUNC(sg.stat_date, 'month'),
        sg.entryid,
        ts.docid,
        sg.customid,
        sg.customer_name,
        sg.customertype_task,
        sg.area_name,
        sg.city_name
),

-- 历史月度销售数据（用于计算近三个月平均）
historical_monthly_sales AS (
    SELECT
        DATE_TRUNC(sg.stat_date, 'month') AS stat_yearmonth,
        sg.entryid,
        ts.docid,
        sg.customid,
        SUM(sg.sales_amount) AS sales_amount
    FROM
        dws.wholesale_sales_goods_d sg
    JOIN
        dim.wholesale_task_set ts 
        ON sg.entryid = ts.entryid 
        AND sg.customid = ts.customid
    WHERE 
        DATE_TRUNC(sg.stat_date, 'month') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 4 MONTH)
        AND DATE_TRUNC(sg.stat_date, 'month') < DATE_TRUNC(CURRENT_DATE(), 'month')
    GROUP BY
        DATE_TRUNC(sg.stat_date, 'month'),
        sg.entryid,
        ts.docid,
        sg.customid
),

-- 近三个月销售数据
prev_3m_sales AS (
    SELECT
        ms.entryid,
        ms.docid,
        ms.customid,
        ms.stat_yearmonth,
        AVG(hms.sales_amount) AS avg_3m_sales_amount
    FROM
        monthly_sales ms
    JOIN
        historical_monthly_sales hms
        ON ms.entryid = hms.entryid
        AND ms.docid = hms.docid
        AND ms.customid = hms.customid
        AND hms.stat_yearmonth BETWEEN DATE_SUB(ms.stat_yearmonth, INTERVAL 3 MONTH) 
                                   AND DATE_SUB(ms.stat_yearmonth, INTERVAL 1 MONTH)
    GROUP BY
        ms.entryid,
        ms.docid,
        ms.customid,
        ms.stat_yearmonth
)

-- 最终结果
SELECT
    ms.stat_yearmonth,
    ms.entryid,
    ms.docid,
    ms.customid,
    ts.task_name,
    e.entry_name,
    ms.customer_name,
    ms.customertype_task,
    ms.area_name,
    ms.city_name,
    ecx.reputation_days,
    ms.sales_amount,
    COALESCE(p3m.avg_3m_sales_amount, 0) AS avg_3m_sales_amount,
    -- 是否异常备货：本月销售额超出近三个月平均销售额的3倍
    CASE 
        WHEN COALESCE(p3m.avg_3m_sales_amount, 0) > 0 
             AND ms.sales_amount > p3m.avg_3m_sales_amount * 3 
        THEN 1 
        ELSE 0 
    END AS is_abnormal_sales,
    -- 备货率偏离率：本月销售额/近三个月平均销售额
    CASE 
        WHEN COALESCE(p3m.avg_3m_sales_amount, 0) > 0 
        THEN ms.sales_amount / p3m.avg_3m_sales_amount 
        ELSE NULL 
    END AS sales_threshold_deviation_rate,
    ms.return_amount,
    -- 是否异常销退：本月销退金额超出近三个月平均销售额的3倍
    CASE 
        WHEN COALESCE(p3m.avg_3m_sales_amount, 0) > 0 
             AND ms.return_amount > p3m.avg_3m_sales_amount * 3 
        THEN 1 
        ELSE 0 
    END AS is_abnormal_return,
    -- 销退率偏离率：本月销退金额/近三个月平均销售额
    CASE 
        WHEN COALESCE(p3m.avg_3m_sales_amount, 0) > 0 
        THEN ms.return_amount / p3m.avg_3m_sales_amount 
        ELSE NULL 
    END AS return_threshold_deviation_rate
FROM
    monthly_sales ms
JOIN
    dim.wholesale_task_set ts 
    ON ms.entryid = ts.entryid 
    AND ms.docid = ts.docid 
    AND ms.customid = ts.customid
JOIN
    dim.entry e 
    ON ms.entryid = e.entryid
    AND e.is_active = 1
LEFT JOIN
    prev_3m_sales p3m 
    ON ms.stat_yearmonth = p3m.stat_yearmonth 
    AND ms.entryid = p3m.entryid 
    AND ms.docid = p3m.docid 
    AND ms.customid = p3m.customid
LEFT JOIN
    dim.entry_customer_xinyu ecx
    ON ms.entryid = ecx.entryid
    AND ms.customid = ecx.customid
    AND ms.stat_yearmonth >= ecx.dw_starttime
    AND ms.stat_yearmonth < ecx.dw_endtime;