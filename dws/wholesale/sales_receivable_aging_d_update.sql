DELETE FROM dws.wholesale_sales_receivable_aging_d
where stat_date > CURRENT_DATE() - INTERVAL 30 DAY;

insert into dws.wholesale_sales_receivable_aging_d (
    stat_date,
    entryid,
    customid,
    unpaid_amount,
    unpaid_order_count,
    unpaid_within_1year,
    unpaid_1to2years,
    unpaid_2to3years,
    unpaid_3to4years,
    unpaid_4to5years,
    unpaid_over_5years
)
WITH all_dates AS (
    -- 获取所有开单和回款日期
    SELECT DISTINCT 
        entryid,
        customid,
        DATE(create_date) AS stat_date 
    FROM dwd.wholesale_order_sales_doc 
    WHERE create_date IS NOT NULL 
    AND sale_mode = '普通销售'
    AND create_date >= CURRENT_DATE() - INTERVAL 30 DAY
    
    UNION
    
    SELECT DISTINCT 
        b.entryid,
        b.customid,
        DATE(b.payment_date) AS stat_date 
    FROM dwd.wholesale_order_sales_dtl a
    JOIN dwd.wholesale_order_repay_dtl b ON a.salesdtlid = b.salesdtlid
    WHERE a.sale_mode = '普通销售'
    AND b.payment_date IS NOT NULL 
    AND b.confirm_date >= CURRENT_DATE() - INTERVAL 30 DAY
),

-- 合并销售和回款事件
all_events AS (
    -- 销售事件（增加应收）
    SELECT
        DATE(s.create_date) AS event_date,
        s.entryid,
        s.customid,
        s.salesid,
        SUM(IFNULL(s.sales_amount,0)) AS amount,
        date(s.create_date) AS original_date,
        'sale' as event_type
    FROM
        dwd.wholesale_order_sales_dtl s
    WHERE s.create_date IS NOT NULL 
    AND s.sale_mode = '普通销售'
    group by DATE(s.create_date),s.entryid,s.customid,s.salesid

    UNION ALL
    
    -- 回款事件（减少应收）
    SELECT
        DATE(s.payment_date) AS event_date,
        s.entryid,
        s.customid,
        s.salesid,
        -SUM(IFNULL(s.payment_amount, 0)) AS amount,
        date(s.payment_date) AS original_date,
        'payment' as event_type
    FROM
        dwd.wholesale_order_repay_dtl s
    JOIN
        dwd.wholesale_order_sales_dtl b ON s.salesdtlid = b.salesdtlid
    WHERE s.payment_date IS NOT NULL
    AND b.sale_mode = '普通销售'
    group by DATE(s.payment_date),s.entryid,s.customid,s.salesid
),

-- 预先计算每个销售单的回款状态
sales_payment_status AS (
    SELECT 
        salesid,
        MIN(is_received) AS order_payment_status,
        MAX(IFNULL(received_time, DATE('9999-12-31'))) AS order_received_time
    FROM 
        dwd.wholesale_sales_receivable_doc
    WHERE 
        salesid IS NOT NULL
    GROUP BY 
        salesid
),

-- 计算每个日期点的销售单状态
sales_status_by_date AS (
    SELECT
        d.stat_date,
        e.entryid,
        e.customid,
        e.salesid,
        e.original_date,
        SUM(CASE WHEN date(e.event_date) <= d.stat_date THEN e.amount ELSE 0 END) AS remaining_amount,
        MIN(CASE WHEN event_type='sale' THEN 
            CASE WHEN ss.order_payment_status = 1 AND date(ss.order_received_time) <= d.stat_date THEN 0 ELSE 1 END
            ELSE 0 END) AS is_unpaid_order
    FROM
        all_dates d
    JOIN
        all_events e ON d.entryid = e.entryid AND d.customid = e.customid AND e.event_date <= d.stat_date
    LEFT JOIN
        sales_payment_status ss ON e.salesid = ss.salesid
    GROUP BY
        d.stat_date, e.entryid, e.customid, e.salesid, e.original_date
),

-- 计算每个日期点的账龄分布
aged_receivables AS (
    SELECT
        stat_date,
        entryid,
        customid,
        salesid,
        remaining_amount,
        is_unpaid_order,
        CASE
            WHEN DATEDIFF(stat_date, original_date) <= 365 THEN remaining_amount
            ELSE 0
        END AS within_1year,
        CASE
            WHEN DATEDIFF(stat_date, original_date) > 365 AND DATEDIFF(stat_date, original_date) <= 730 THEN remaining_amount
            ELSE 0
        END AS _1to2years,
        CASE
            WHEN DATEDIFF(stat_date, original_date) > 730 AND DATEDIFF(stat_date, original_date) <= 1095 THEN remaining_amount
            ELSE 0
        END AS _2to3years,
        CASE
            WHEN DATEDIFF(stat_date, original_date) > 1095 AND DATEDIFF(stat_date, original_date) <= 1460 THEN remaining_amount
            ELSE 0
        END AS _3to4years,
        CASE
            WHEN DATEDIFF(stat_date, original_date) > 1460 AND DATEDIFF(stat_date, original_date) <= 1825 THEN remaining_amount
            ELSE 0
        END AS _4to5years,
        CASE
            WHEN DATEDIFF(stat_date, original_date) > 1825 THEN remaining_amount
            ELSE 0
        END AS over_5years
    FROM
        sales_status_by_date
)

-- 按日期、独立单元、客户分组，汇总账龄数据
SELECT
    stat_date,
    entryid,
    customid,
    SUM(remaining_amount) AS unpaid_amount,
    SUM(is_unpaid_order) AS unpaid_order_count,
    SUM(within_1year) AS unpaid_within_1year,
    SUM(_1to2years) AS unpaid_1to2years,
    SUM(_2to3years) AS unpaid_2to3years,
    SUM(_3to4years) AS unpaid_3to4years,
    SUM(_4to5years) AS unpaid_4to5years,
    SUM(over_5years) AS unpaid_over_5years
FROM
    aged_receivables
GROUP BY
    stat_date, entryid, customid;
