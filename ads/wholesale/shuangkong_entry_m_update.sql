INSERT INTO ads.shuangkong_entry_m (
    stat_yearmonth,
    entryid,
    entry_name,
    province_name,
    city_name,
    current_sales_amount,
    current_sales_gross_profit,
    current_settle_amount,
    current_repaid_amount,
    prev_sales_amount,
    prev_sales_gross_profit,
    prev_settle_amount,
    prev_repaid_amount,
    avg_3m_sales_amount,
    avg_3m_sales_gross_profit,
    avg_3m_settle_amount,
    avg_3m_repaid_amount
)
WITH 
-- 销售数据月度汇总
sales_monthly AS (
    SELECT
        DATE_TRUNC(stat_date, 'month') AS stat_yearmonth,
        entryid,
        MAX(entry_name) AS entry_name,
        MAX(province_name) AS province_name,
        '' AS city_name, -- 销售数据中可能没有城市信息，使用空字符串
        SUM(sales_amount) AS sales_amount,
        SUM(sales_gross_profit) AS sales_gross_profit
    FROM
        dws.wholesale_sales_detail_d
    WHERE
        DATE_TRUNC(stat_date, 'month') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 5 MONTH) 
        AND DATE_TRUNC(stat_date, 'month') <= DATE_TRUNC(CURRENT_DATE(), 'month')
    GROUP BY
        DATE_TRUNC(stat_date, 'month'),
        entryid
),

-- 结算数据月度汇总
settle_monthly AS (
    SELECT
        DATE_TRUNC(stat_date, 'month') AS stat_yearmonth,
        entryid,
        SUM(settle_amount) AS settle_amount
    FROM
        dws.wholesale_settle_detail_d
    WHERE
        DATE_TRUNC(stat_date, 'month') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 5 MONTH) 
        AND DATE_TRUNC(stat_date, 'month') <= DATE_TRUNC(CURRENT_DATE(), 'month')
    GROUP BY
        DATE_TRUNC(stat_date, 'month'),
        entryid
),

-- 回款数据月度汇总
repay_monthly AS (
    SELECT
        DATE_TRUNC(stat_date, 'month') AS stat_yearmonth,
        entryid,
        SUM(repaid_amount) AS repaid_amount
    FROM
        dws.wholesale_repay_detail_d
    WHERE
        DATE_TRUNC(stat_date, 'month') >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 5 MONTH) 
        AND DATE_TRUNC(stat_date, 'month') <= DATE_TRUNC(CURRENT_DATE(), 'month')
    GROUP BY
        DATE_TRUNC(stat_date, 'month'),
        entryid
),

-- 所有月份数据合并
all_monthly_data AS (
    SELECT
        sm.stat_yearmonth,
        sm.entryid,
        sm.entry_name,
        sm.province_name,
        sm.city_name,
        sm.sales_amount,
        sm.sales_gross_profit,
        COALESCE(stm.settle_amount, 0) AS settle_amount,
        COALESCE(rm.repaid_amount, 0) AS repaid_amount
    FROM
        sales_monthly sm
    LEFT JOIN
        settle_monthly stm ON sm.stat_yearmonth = stm.stat_yearmonth AND sm.entryid = stm.entryid
    LEFT JOIN
        repay_monthly rm ON sm.stat_yearmonth = rm.stat_yearmonth AND sm.entryid = rm.entryid
),

-- 当前月份
current_month AS (
    SELECT
        amd.stat_yearmonth,
        amd.entryid,
        amd.entry_name,
        amd.province_name,
        amd.city_name,
        amd.sales_amount AS current_sales_amount,
        amd.sales_gross_profit AS current_sales_gross_profit,
        amd.settle_amount AS current_settle_amount,
        amd.repaid_amount AS current_repaid_amount
    FROM
        all_monthly_data amd
),

-- 上个月数据
prev_month AS (
    SELECT
        DATE_SUB(amd.stat_yearmonth, INTERVAL 1 MONTH) AS prev_month,
        amd.entryid,
        amd.sales_amount AS prev_sales_amount,
        amd.sales_gross_profit AS prev_sales_gross_profit,
        amd.settle_amount AS prev_settle_amount,
        amd.repaid_amount AS prev_repaid_amount
    FROM
        all_monthly_data amd
),

-- 近三个月平均值（实际是前1-4个月的平均值）
avg_3_months AS (
    SELECT
        current_month.stat_yearmonth,
        current_month.entryid,
        AVG(prev_months.sales_amount) AS avg_3m_sales_amount,
        AVG(prev_months.sales_gross_profit) AS avg_3m_sales_gross_profit,
        AVG(prev_months.settle_amount) AS avg_3m_settle_amount,
        AVG(prev_months.repaid_amount) AS avg_3m_repaid_amount
    FROM
        all_monthly_data current_month
    JOIN
        all_monthly_data prev_months 
        ON current_month.entryid = prev_months.entryid
        AND prev_months.stat_yearmonth >= DATE_SUB(current_month.stat_yearmonth, INTERVAL 4 MONTH) 
        AND prev_months.stat_yearmonth <= DATE_SUB(current_month.stat_yearmonth, INTERVAL 1 MONTH)
    GROUP BY
        current_month.stat_yearmonth,
        current_month.entryid
)

-- 最终结果
SELECT
    cm.stat_yearmonth,
    cm.entryid,
    cm.entry_name,
    cm.province_name,
    cm.city_name,
    cm.current_sales_amount,
    cm.current_sales_gross_profit,
    cm.current_settle_amount,
    cm.current_repaid_amount,
    COALESCE(pm.prev_sales_amount, 0) AS prev_sales_amount,
    COALESCE(pm.prev_sales_gross_profit, 0) AS prev_sales_gross_profit,
    COALESCE(pm.prev_settle_amount, 0) AS prev_settle_amount,
    COALESCE(pm.prev_repaid_amount, 0) AS prev_repaid_amount,
    COALESCE(a3m.avg_3m_sales_amount, 0) AS avg_3m_sales_amount,
    COALESCE(a3m.avg_3m_sales_gross_profit, 0) AS avg_3m_sales_gross_profit,
    COALESCE(a3m.avg_3m_settle_amount, 0) AS avg_3m_settle_amount,
    COALESCE(a3m.avg_3m_repaid_amount, 0) AS avg_3m_repaid_amount
FROM
    current_month cm
LEFT JOIN
    prev_month pm ON cm.stat_yearmonth = pm.prev_month AND cm.entryid = pm.entryid
LEFT JOIN
    avg_3_months a3m ON cm.stat_yearmonth = a3m.stat_yearmonth AND cm.entryid = a3m.entryid
WHERE cm.stat_yearmonth = DATE_TRUNC(CURRENT_DATE(), 'month')
OR cm.stat_yearmonth = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 1 MONTH);