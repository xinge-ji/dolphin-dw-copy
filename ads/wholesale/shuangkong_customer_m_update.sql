INSERT INTO ads.shuangkong_customer_m (
    stat_yearmonth,
    entryid,
    customid,
    jicai_type,
    nianbao_type,
    entry_name,
    province_name,
    customer_name,
    customertype_name,
    is_shuangwanjia,
    customertype_group,
    current_sales_amount,
    current_sales_gross_profit,
    current_settle_amount,
    current_repaid_amount,
    prev_sales_amount,
    prev_sales_gross_profit,
    prev_settle_amount,
    prev_order_settle_time,
    prev_jicai_order_settle_time,
    prev_non_jicai_order_settle_time,
    prev_repaid_amount,
    prev_order_repaid_time,
    prev_jicai_order_repaid_time,
    prev_non_jicai_order_repaid_time,
    avg_3m_sales_amount,
    avg_3m_sales_gross_profit,
    avg_3m_settle_amount,
    avg_3m_order_settle_time,
    avg_3m_jicai_order_settle_time,
    avg_3m_non_jicai_order_settle_time,
    avg_3m_repaid_amount,
    avg_3m_order_repaid_time,
    avg_3m_jicai_order_repaid_time,
    avg_3m_non_jicai_order_repaid_time,
    unpaid_amount,
    unpaid_order_count,
    unpaid_within_1year,
    unpaid_1to2years,
    unpaid_2to3years,
    unpaid_3to4years,
    unpaid_4to5years,
    unpaid_over_5years,
    bad_debt_reserve,
    is_key_bad_debt_customer
)
WITH 
-- 获取当前月份和上月
current_period AS (
    SELECT 
        DATE_TRUNC(CURRENT_DATE(), 'month') AS current_month,
        DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'month'), INTERVAL 1 MONTH) AS prev_month,
        LAST_DAY(CURRENT_DATE()) AS current_month_last_day
),

-- 销售数据月度汇总
sales_monthly AS (
    SELECT
        DATE_TRUNC(stat_date, 'month') AS stat_yearmonth,
        entryid,
        customid,
        COALESCE(jicai_type, 'UNKNOWN') AS jicai_type,
        COALESCE(nianbao_type, 'UNKNOWN') AS nianbao_type,
        MAX(entry_name) AS entry_name,
        MAX(province_name) AS province_name,
        MAX(customer_name) AS customer_name,
        MAX(customertype_name) AS customertype_name,
        c.is_shuangwanjia,
        MAX(customertype_group) AS customertype_group,
        SUM(sales_amount) AS sales_amount,
        SUM(sales_gross_profit) AS sales_gross_profit
    FROM
        dws.wholesale_sales_detail_d wos
    LEFT JOIN
        (SELECT customid, MAX(is_shuangwanjia) AS is_shuangwanjia FROM dim.customer GROUP BY customid) AS c ON wos.customid = c.customid
    WHERE
        DATE_TRUNC(stat_date, 'month') >= DATE_SUB((SELECT current_month FROM current_period), INTERVAL 5 MONTH) 
        AND DATE_TRUNC(stat_date, 'month') <= (SELECT current_month FROM current_period)
    GROUP BY
        DATE_TRUNC(stat_date, 'month'),
        entryid,
        customid,
        COALESCE(jicai_type, 'UNKNOWN'),
        COALESCE(nianbao_type, 'UNKNOWN'),
        c.is_shuangwanjia
),

-- 结算数据月度汇总
settle_monthly AS (
    SELECT
        DATE_TRUNC(stat_date, 'month') AS stat_yearmonth,
        entryid,
        customid,
        COALESCE(jicai_type, 'UNKNOWN') AS jicai_type,
        COALESCE(nianbao_type, 'UNKNOWN') AS nianbao_type,
        SUM(settle_amount) AS settle_amount,
        AVG(avg_order_settle_time) AS avg_order_settle_time,
        CASE 
            WHEN LOWER(jicai_type) LIKE '%集采%' OR LOWER(jicai_type) = '集采' 
            THEN AVG(avg_order_settle_time) 
            ELSE 0 
        END AS jicai_order_settle_time,
        CASE 
            WHEN LOWER(jicai_type) NOT LIKE '%集采%' AND LOWER(jicai_type) != '集采' 
            THEN AVG(avg_order_settle_time) 
            ELSE 0 
        END AS non_jicai_order_settle_time
    FROM
        dws.wholesale_settle_detail_d
    WHERE
        DATE_TRUNC(stat_date, 'month') >= DATE_SUB((SELECT current_month FROM current_period), INTERVAL 5 MONTH) 
        AND DATE_TRUNC(stat_date, 'month') <= (SELECT current_month FROM current_period)
    GROUP BY
        DATE_TRUNC(stat_date, 'month'),
        entryid,
        customid,
        COALESCE(jicai_type, 'UNKNOWN'),
        COALESCE(nianbao_type, 'UNKNOWN')
),

-- 回款数据月度汇总
repay_monthly AS (
    SELECT
        DATE_TRUNC(stat_date, 'month') AS stat_yearmonth,
        entryid,
        customid,
        COALESCE(jicai_type, 'UNKNOWN') AS jicai_type,
        COALESCE(nianbao_type, 'UNKNOWN') AS nianbao_type,
        SUM(repaid_amount) AS repaid_amount,
        AVG(avg_order_repaid_time) AS avg_order_repaid_time,
        CASE 
            WHEN LOWER(jicai_type) LIKE '%集采%' OR LOWER(jicai_type) = '集采' 
            THEN AVG(avg_order_repaid_time) 
            ELSE 0 
        END AS jicai_order_repaid_time,
        CASE 
            WHEN LOWER(jicai_type) NOT LIKE '%集采%' AND LOWER(jicai_type) != '集采' 
            THEN AVG(avg_order_repaid_time) 
            ELSE 0 
        END AS non_jicai_order_repaid_time
    FROM
        dws.wholesale_repay_detail_d
    WHERE
        DATE_TRUNC(stat_date, 'month') >= DATE_SUB((SELECT current_month FROM current_period), INTERVAL 5 MONTH) 
        AND DATE_TRUNC(stat_date, 'month') <= (SELECT current_month FROM current_period)
    GROUP BY
        DATE_TRUNC(stat_date, 'month'),
        entryid,
        customid,
        COALESCE(jicai_type, 'UNKNOWN'),
        COALESCE(nianbao_type, 'UNKNOWN')
),

-- 应收账款账龄数据
receivable_aging AS (
    SELECT
        LAST_DAY(DATE_TRUNC(stat_date, 'month')) AS stat_yearmonth,
        CAST(entryid AS BIGINT) AS entryid,
        CAST(customid AS BIGINT) AS customid,
        SUM(unpaid_amount) AS unpaid_amount,
        SUM(unpaid_order_count) AS unpaid_order_count,
        SUM(unpaid_within_1year) AS unpaid_within_1year,
        SUM(unpaid_1to2years) AS unpaid_1to2years,
        SUM(unpaid_2to3years) AS unpaid_2to3years,
        SUM(unpaid_3to4years) AS unpaid_3to4years,
        SUM(unpaid_4to5years) AS unpaid_4to5years,
        SUM(unpaid_over_5years) AS unpaid_over_5years,
        -- 计算坏账计提
        SUM(unpaid_1to2years) * 0.05 + 
        SUM(unpaid_2to3years) * 0.3 + 
        SUM(unpaid_3to4years) * 0.5 + 
        SUM(unpaid_4to5years) * 0.7 + 
        SUM(unpaid_over_5years) AS bad_debt_reserve,
        -- 计算超一年应收账款总额
        SUM(unpaid_1to2years) + SUM(unpaid_2to3years) + SUM(unpaid_3to4years) + 
        SUM(unpaid_4to5years) + SUM(unpaid_over_5years) AS over_1year_unpaid,
        -- 计算超两年应收账款总额
        SUM(unpaid_2to3years) + SUM(unpaid_3to4years) + 
        SUM(unpaid_4to5years) + SUM(unpaid_over_5years) AS over_2year_unpaid
    FROM
        dws.wholesale_sales_receivable_aging_d
    WHERE
        stat_date = (
            SELECT MAX(stat_date) 
            FROM dws.wholesale_sales_receivable_aging_d 
            WHERE DATE_TRUNC(stat_date, 'month') <= (SELECT current_month FROM current_period)
        )
    GROUP BY
        LAST_DAY(DATE_TRUNC(stat_date, 'month')),
        entryid,
        customid
),

-- 所有月份数据合并
all_monthly_data AS (
    SELECT
        sm.stat_yearmonth,
        sm.entryid,
        sm.customid,
        sm.jicai_type,
        sm.nianbao_type,
        sm.entry_name,
        sm.province_name,
        sm.customer_name,
        sm.customertype_name,
        sm.is_shuangwanjia,
        sm.customertype_group,
        sm.sales_amount,
        sm.sales_gross_profit,
        COALESCE(stm.settle_amount, 0) AS settle_amount,
        COALESCE(stm.avg_order_settle_time, 0) AS avg_order_settle_time,
        COALESCE(stm.jicai_order_settle_time, 0) AS jicai_order_settle_time,
        COALESCE(stm.non_jicai_order_settle_time, 0) AS non_jicai_order_settle_time,
        COALESCE(rm.repaid_amount, 0) AS repaid_amount,
        COALESCE(rm.avg_order_repaid_time, 0) AS avg_order_repaid_time,
        COALESCE(rm.jicai_order_repaid_time, 0) AS jicai_order_repaid_time,
        COALESCE(rm.non_jicai_order_repaid_time, 0) AS non_jicai_order_repaid_time
    FROM
        sales_monthly sm
    LEFT JOIN
        settle_monthly stm 
        ON sm.stat_yearmonth = stm.stat_yearmonth 
        AND sm.entryid = stm.entryid
        AND sm.customid = stm.customid
        AND sm.jicai_type = stm.jicai_type
        AND sm.nianbao_type = stm.nianbao_type
    LEFT JOIN
        repay_monthly rm 
        ON sm.stat_yearmonth = rm.stat_yearmonth 
        AND sm.entryid = rm.entryid
        AND sm.customid = rm.customid
        AND sm.jicai_type = rm.jicai_type
        AND sm.nianbao_type = rm.nianbao_type
),

-- 当前月份
current_month_data AS (
    SELECT
        amd.stat_yearmonth,
        amd.entryid,
        amd.customid,
        amd.jicai_type,
        amd.nianbao_type,
        amd.entry_name,
        amd.province_name,
        amd.customer_name,
        amd.customertype_name,
        amd.is_shuangwanjia,
        amd.customertype_group,
        amd.sales_amount AS current_sales_amount,
        amd.sales_gross_profit AS current_sales_gross_profit,
        amd.settle_amount AS current_settle_amount,
        amd.repaid_amount AS current_repaid_amount
    FROM
        all_monthly_data amd
    WHERE
        amd.stat_yearmonth IN ((SELECT current_month FROM current_period), (SELECT prev_month FROM current_period))
),

-- 上个月数据
prev_month_data AS (
    SELECT
        DATE_ADD(amd.stat_yearmonth, INTERVAL 1 MONTH) AS next_month,
        amd.entryid,
        amd.customid,
        amd.jicai_type,
        amd.nianbao_type,
        amd.sales_amount AS prev_sales_amount,
        amd.sales_gross_profit AS prev_sales_gross_profit,
        amd.settle_amount AS prev_settle_amount,
        amd.avg_order_settle_time AS prev_order_settle_time,
        amd.jicai_order_settle_time AS prev_jicai_order_settle_time,
        amd.non_jicai_order_settle_time AS prev_non_jicai_order_settle_time,
        amd.repaid_amount AS prev_repaid_amount,
        amd.avg_order_repaid_time AS prev_order_repaid_time,
        amd.jicai_order_repaid_time AS prev_jicai_order_repaid_time,
        amd.non_jicai_order_repaid_time AS prev_non_jicai_order_repaid_time
    FROM
        all_monthly_data amd
    WHERE
        amd.stat_yearmonth IN (
            (SELECT prev_month FROM current_period),
            DATE_SUB((SELECT prev_month FROM current_period), INTERVAL 1 MONTH)
        )
),

-- 近三个月平均值（实际是前1-4个月的平均值）
avg_3_months AS (
    SELECT
        target_month.stat_yearmonth,
        target_month.entryid,
        target_month.customid,
        target_month.jicai_type,
        target_month.nianbao_type,
        AVG(prev_months.sales_amount) AS avg_3m_sales_amount,
        AVG(prev_months.sales_gross_profit) AS avg_3m_sales_gross_profit,
        AVG(prev_months.settle_amount) AS avg_3m_settle_amount,
        AVG(prev_months.avg_order_settle_time) AS avg_3m_order_settle_time,
        AVG(prev_months.jicai_order_settle_time) AS avg_3m_jicai_order_settle_time,
        AVG(prev_months.non_jicai_order_settle_time) AS avg_3m_non_jicai_order_settle_time,
        AVG(prev_months.repaid_amount) AS avg_3m_repaid_amount,
        AVG(prev_months.avg_order_repaid_time) AS avg_3m_order_repaid_time,
        AVG(prev_months.jicai_order_repaid_time) AS avg_3m_jicai_order_repaid_time,
        AVG(prev_months.non_jicai_order_repaid_time) AS avg_3m_non_jicai_order_repaid_time
    FROM
        all_monthly_data target_month
    JOIN
        all_monthly_data prev_months 
        ON target_month.entryid = prev_months.entryid
        AND target_month.customid = prev_months.customid
        AND target_month.jicai_type = prev_months.jicai_type
        AND target_month.nianbao_type = prev_months.nianbao_type
        AND prev_months.stat_yearmonth BETWEEN DATE_SUB(target_month.stat_yearmonth, INTERVAL 4 MONTH) 
                                           AND DATE_SUB(target_month.stat_yearmonth, INTERVAL 1 MONTH)
    WHERE
        target_month.stat_yearmonth IN ((SELECT current_month FROM current_period), (SELECT prev_month FROM current_period))
    GROUP BY
        target_month.stat_yearmonth,
        target_month.entryid,
        target_month.customid,
        target_month.jicai_type,
        target_month.nianbao_type
)

-- 最终结果，只选择当前月和上月的数据
SELECT
    cm.stat_yearmonth,
    cm.entryid,
    cm.customid,
    cm.jicai_type,
    cm.nianbao_type,
    cm.entry_name,
    cm.province_name,
    cm.customer_name,
    cm.customertype_name,
    cm.is_shuangwanjia,
    cm.customertype_group,
    cm.current_sales_amount,
    cm.current_sales_gross_profit,
    cm.current_settle_amount,
    cm.current_repaid_amount,
    COALESCE(pm.prev_sales_amount, 0) AS prev_sales_amount,
    COALESCE(pm.prev_sales_gross_profit, 0) AS prev_sales_gross_profit,
    COALESCE(pm.prev_settle_amount, 0) AS prev_settle_amount,
    COALESCE(pm.prev_order_settle_time, 0) AS prev_order_settle_time,
    COALESCE(pm.prev_jicai_order_settle_time, 0) AS prev_jicai_order_settle_time,
    COALESCE(pm.prev_non_jicai_order_settle_time, 0) AS prev_non_jicai_order_settle_time,
    COALESCE(pm.prev_repaid_amount, 0) AS prev_repaid_amount,
    COALESCE(pm.prev_order_repaid_time, 0) AS prev_order_repaid_time,
    COALESCE(pm.prev_jicai_order_repaid_time, 0) AS prev_jicai_order_repaid_time,
    COALESCE(pm.prev_non_jicai_order_repaid_time, 0) AS prev_non_jicai_order_repaid_time,
    COALESCE(a3m.avg_3m_sales_amount, 0) AS avg_3m_sales_amount,
    COALESCE(a3m.avg_3m_sales_gross_profit, 0) AS avg_3m_sales_gross_profit,
    COALESCE(a3m.avg_3m_settle_amount, 0) AS avg_3m_settle_amount,
    COALESCE(a3m.avg_3m_order_settle_time, 0) AS avg_3m_order_settle_time,
    COALESCE(a3m.avg_3m_jicai_order_settle_time, 0) AS avg_3m_jicai_order_settle_time,
    COALESCE(a3m.avg_3m_non_jicai_order_settle_time, 0) AS avg_3m_non_jicai_order_settle_time,
    COALESCE(a3m.avg_3m_repaid_amount, 0) AS avg_3m_repaid_amount,
    COALESCE(a3m.avg_3m_order_repaid_time, 0) AS avg_3m_order_repaid_time,
    COALESCE(a3m.avg_3m_jicai_order_repaid_time, 0) AS avg_3m_jicai_order_repaid_time,
    COALESCE(a3m.avg_3m_non_jicai_order_repaid_time, 0) AS avg_3m_non_jicai_order_repaid_time,
    COALESCE(ra.unpaid_amount, 0) AS unpaid_amount,
    COALESCE(ra.unpaid_order_count, 0) AS unpaid_order_count,
    COALESCE(ra.unpaid_within_1year, 0) AS unpaid_within_1year,
    COALESCE(ra.unpaid_1to2years, 0) AS unpaid_1to2years,
    COALESCE(ra.unpaid_2to3years, 0) AS unpaid_2to3years,
    COALESCE(ra.unpaid_3to4years, 0) AS unpaid_3to4years,
    COALESCE(ra.unpaid_4to5years, 0) AS unpaid_4to5years,
    COALESCE(ra.unpaid_over_5years, 0) AS unpaid_over_5years,
    COALESCE(ra.bad_debt_reserve, 0) AS bad_debt_reserve,
    -- 计算是否为坏账客户
    CASE
        -- 公立等级：(超一年应收账款 ≥ 100万) | (坏账计提 ≥ 20万)
        WHEN cm.customertype_name='公立等级' AND 
             (COALESCE(ra.over_1year_unpaid, 0) >= 1000000 OR COALESCE(ra.bad_debt_reserve, 0) >= 200000) 
        THEN 1
        -- 公立基层：(超两年应收账款 > 0万) | (坏账计提 ≥ 10万)
        WHEN cm.customertype_name='公立基层' AND 
             (COALESCE(ra.over_2year_unpaid, 0) > 0 OR COALESCE(ra.bad_debt_reserve, 0) >= 100000) 
        THEN 1
        -- 民营：(超一年应收账款 ≥ 10万) | (坏账计提 ≥ 3万)
        WHEN cm.customertype_name='民营' AND 
             (COALESCE(ra.over_1year_unpaid, 0) >= 100000 OR COALESCE(ra.bad_debt_reserve, 0) >= 30000) 
        THEN 1
        ELSE 0
    END AS is_key_bad_debt_customer
FROM
    current_month_data cm
LEFT JOIN
    prev_month_data pm 
    ON cm.stat_yearmonth = pm.next_month 
    AND cm.entryid = pm.entryid
    AND cm.customid = pm.customid
    AND cm.jicai_type = pm.jicai_type
    AND cm.nianbao_type = pm.nianbao_type
LEFT JOIN
    avg_3_months a3m 
    ON cm.stat_yearmonth = a3m.stat_yearmonth 
    AND cm.entryid = a3m.entryid
    AND cm.customid = a3m.customid
    AND cm.jicai_type = a3m.jicai_type
    AND cm.nianbao_type = a3m.nianbao_type
LEFT JOIN
    receivable_aging ra
    ON cm.stat_yearmonth = ra.stat_yearmonth
    AND cm.entryid = ra.entryid
    AND cm.customid = ra.customid
WHERE 
    -- 只插入当前月和上月的数据
    cm.stat_yearmonth = (SELECT current_month FROM current_period)
    OR cm.stat_yearmonth = (SELECT prev_month FROM current_period);