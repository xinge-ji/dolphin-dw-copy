DELETE FROM ads.shuangkong_customer_m
WHERE stat_yearmonth IN (
    DATE_TRUNC(CURRENT_DATE(), 'MONTH'),
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 1 MONTH)
);

INSERT INTO ads.shuangkong_customer_m (
    stat_yearmonth,
    entryid,
    customid,
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
    prev_order_item_settle_time,
    prev_jicai_order_item_settle_time,
    prev_non_jicai_order_item_settle_time,
    prev_repaid_amount,
    prev_order_item_repaid_time,
    prev_jicai_order_item_repaid_time,
    prev_non_jicai_order_item_repaid_time,
    avg_3m_sales_amount,
    avg_3m_sales_gross_profit,
    avg_3m_settle_amount,
    avg_3m_order_item_settle_time,
    avg_3m_jicai_order_item_settle_time,
    avg_3m_non_jicai_order_item_settle_time,
    avg_3m_repaid_amount,
    avg_3m_order_item_repaid_time,
    avg_3m_jicai_order_item_repaid_time,
    avg_3m_non_jicai_order_item_repaid_time,
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
        DATE_TRUNC(wos.stat_date, 'month') AS stat_yearmonth,
        wos.entryid,
        wos.customid,
        MAX(wos.entry_name) AS entry_name,
        MAX(wos.province_name) AS province_name,
        MAX(wos.customer_name) AS customer_name,
        MAX(wos.customertype_name) AS customertype_name,
        COALESCE(c.is_shuangwanjia, 0) AS is_shuangwanjia,
        MAX(wos.customertype_group) AS customertype_group,
        SUM(wos.sales_amount) AS sales_amount,
        SUM(wos.sales_gross_profit) AS sales_gross_profit
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
        COALESCE(c.is_shuangwanjia, 0)
),

-- 结算数据月度汇总
settle_monthly AS (
    SELECT
        DATE_TRUNC(stat_date, 'month') AS stat_yearmonth,
        entryid,
        customid,
        SUM(settle_amount) AS settle_amount,
        -- 计算结算用时
        CASE 
            WHEN SUM(settled_order_item_count) > 0 
            THEN SUM(avg_order_item_settle_time * settled_order_item_count) / SUM(settled_order_item_count)
            ELSE 0 
        END AS avg_order_item_settle_time,
        -- 集采结算用时
        CASE 
            WHEN SUM(CASE WHEN COALESCE(jicai_type, '非集采') != '非集采' THEN settled_order_item_count ELSE 0 END) > 0
            THEN SUM(CASE WHEN COALESCE(jicai_type, '非集采') != '非集采' THEN avg_order_item_settle_time * settled_order_item_count ELSE 0 END) / 
                 SUM(CASE WHEN COALESCE(jicai_type, '非集采') != '非集采' THEN settled_order_item_count ELSE 0 END)
            ELSE 0 
        END AS jicai_order_item_settle_time,
        -- 非集采结算用时
        CASE 
            WHEN SUM(CASE WHEN COALESCE(jicai_type, '非集采') = '非集采' THEN settled_order_item_count ELSE 0 END) > 0
            THEN SUM(CASE WHEN COALESCE(jicai_type, '非集采') = '非集采' THEN avg_order_item_settle_time * settled_order_item_count ELSE 0 END) / 
                 SUM(CASE WHEN COALESCE(jicai_type, '非集采') = '非集采' THEN settled_order_item_count ELSE 0 END)
            ELSE 0 
        END AS non_jicai_order_item_settle_time
    FROM
        dws.wholesale_settle_detail_d
    WHERE
        DATE_TRUNC(stat_date, 'month') >= DATE_SUB((SELECT current_month FROM current_period), INTERVAL 5 MONTH) 
        AND DATE_TRUNC(stat_date, 'month') <= (SELECT current_month FROM current_period)
    GROUP BY
        DATE_TRUNC(stat_date, 'month'),
        entryid,
        customid
),

-- 回款数据月度汇总
repay_monthly AS (
    SELECT
        DATE_TRUNC(stat_date, 'month') AS stat_yearmonth,
        entryid,
        customid,
        SUM(repaid_amount) AS repaid_amount,
        -- 使用加权平均计算回款用时
        CASE 
            WHEN SUM(repaid_order_item_count) > 0 
            THEN SUM(avg_order_item_repaid_time * repaid_order_item_count) / SUM(repaid_order_item_count)
            ELSE 0 
        END AS avg_order_item_repaid_time,
        -- 集采回款用时
        CASE 
            WHEN SUM(CASE WHEN COALESCE(jicai_type, '非集采') != '非集采' THEN repaid_order_item_count ELSE 0 END) > 0
            THEN SUM(CASE WHEN COALESCE(jicai_type, '非集采') != '非集采' THEN avg_order_item_repaid_time * repaid_order_item_count ELSE 0 END) / 
                 SUM(CASE WHEN COALESCE(jicai_type, '非集采') != '非集采' THEN repaid_order_item_count ELSE 0 END)
            ELSE 0 
        END AS jicai_order_item_repaid_time,
        -- 非集采回款用时
        CASE 
            WHEN SUM(CASE WHEN COALESCE(jicai_type, '非集采') = '非集采' THEN repaid_order_item_count ELSE 0 END) > 0
            THEN SUM(CASE WHEN COALESCE(jicai_type, '非集采') = '非集采' THEN avg_order_item_repaid_time * repaid_order_item_count ELSE 0 END) / 
                 SUM(CASE WHEN COALESCE(jicai_type, '非集采') = '非集采' THEN repaid_order_item_count ELSE 0 END)
            ELSE 0 
        END AS non_jicai_order_item_repaid_time
    FROM
        dws.wholesale_repay_detail_d
    WHERE
        DATE_TRUNC(stat_date, 'month') >= DATE_SUB((SELECT current_month FROM current_period), INTERVAL 5 MONTH) 
        AND DATE_TRUNC(stat_date, 'month') <= (SELECT current_month FROM current_period)
    GROUP BY
        DATE_TRUNC(stat_date, 'month'),
        entryid,
        customid
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
        sm.entry_name,
        sm.province_name,
        sm.customer_name,
        sm.customertype_name,
        sm.is_shuangwanjia,
        sm.customertype_group,
        sm.sales_amount,
        sm.sales_gross_profit,
        COALESCE(stm.settle_amount, 0) AS settle_amount,
        COALESCE(stm.avg_order_item_settle_time, 0) AS avg_order_item_settle_time,
        COALESCE(stm.jicai_order_item_settle_time, 0) AS jicai_order_item_settle_time,
        COALESCE(stm.non_jicai_order_item_settle_time, 0) AS non_jicai_order_item_settle_time,
        COALESCE(rm.repaid_amount, 0) AS repaid_amount,
        COALESCE(rm.avg_order_item_repaid_time, 0) AS avg_order_item_repaid_time,
        COALESCE(rm.jicai_order_item_repaid_time, 0) AS jicai_order_item_repaid_time,
        COALESCE(rm.non_jicai_order_item_repaid_time, 0) AS non_jicai_order_item_repaid_time
    FROM
        sales_monthly sm
    LEFT JOIN
        settle_monthly stm 
        ON sm.stat_yearmonth = stm.stat_yearmonth 
        AND sm.entryid = stm.entryid
        AND sm.customid = stm.customid
    LEFT JOIN
        repay_monthly rm 
        ON sm.stat_yearmonth = rm.stat_yearmonth 
        AND sm.entryid = rm.entryid
        AND sm.customid = rm.customid
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
),

-- 上个月数据
prev_month_data AS (
    SELECT
        DATE_ADD(amd.stat_yearmonth, INTERVAL 1 MONTH) AS next_month,
        amd.entryid,
        amd.customid,
        amd.sales_amount AS prev_sales_amount,
        amd.sales_gross_profit AS prev_sales_gross_profit,
        amd.settle_amount AS prev_settle_amount,
        amd.avg_order_item_settle_time AS prev_order_item_settle_time,
        amd.jicai_order_item_settle_time AS prev_jicai_order_item_settle_time,
        amd.non_jicai_order_item_settle_time AS prev_non_jicai_order_item_settle_time,
        amd.repaid_amount AS prev_repaid_amount,
        amd.avg_order_item_repaid_time AS prev_order_item_repaid_time,
        amd.jicai_order_item_repaid_time AS prev_jicai_order_item_repaid_time,
        amd.non_jicai_order_item_repaid_time AS prev_non_jicai_order_item_repaid_time
    FROM
        all_monthly_data amd
),

-- 近三个月平均值（实际是前1-4个月的平均值）
avg_3_months AS (
    SELECT
        target_month.stat_yearmonth,
        target_month.entryid,
        target_month.customid,
        AVG(prev_months.sales_amount) AS avg_3m_sales_amount,
        AVG(prev_months.sales_gross_profit) AS avg_3m_sales_gross_profit,
        AVG(prev_months.settle_amount) AS avg_3m_settle_amount,
        AVG(prev_months.avg_order_item_settle_time) AS avg_3m_order_item_settle_time,
        AVG(prev_months.jicai_order_item_settle_time) AS avg_3m_jicai_order_item_settle_time,
        AVG(prev_months.non_jicai_order_item_settle_time) AS avg_3m_non_jicai_order_item_settle_time,
        AVG(prev_months.repaid_amount) AS avg_3m_repaid_amount,
        AVG(prev_months.avg_order_item_repaid_time) AS avg_3m_order_item_repaid_time,
        AVG(prev_months.jicai_order_item_repaid_time) AS avg_3m_jicai_order_item_repaid_time,
        AVG(prev_months.non_jicai_order_item_repaid_time) AS avg_3m_non_jicai_order_item_repaid_time
    FROM
        all_monthly_data target_month
    JOIN
        all_monthly_data prev_months 
        ON target_month.entryid = prev_months.entryid
        AND target_month.customid = prev_months.customid
        AND prev_months.stat_yearmonth BETWEEN DATE_SUB(target_month.stat_yearmonth, INTERVAL 4 MONTH) 
                                           AND DATE_SUB(target_month.stat_yearmonth, INTERVAL 1 MONTH)
    GROUP BY
        target_month.stat_yearmonth,
        target_month.entryid,
        target_month.customid
)

-- 最终结果
SELECT
    cm.stat_yearmonth,
    cm.entryid,
    cm.customid,
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
    COALESCE(pm.prev_order_item_settle_time, 0) AS prev_order_item_settle_time,
    COALESCE(pm.prev_jicai_order_item_settle_time, 0) AS prev_jicai_order_item_settle_time,
    COALESCE(pm.prev_non_jicai_order_item_settle_time, 0) AS prev_non_jicai_order_item_settle_time,
    COALESCE(pm.prev_repaid_amount, 0) AS prev_repaid_amount,
    COALESCE(pm.prev_order_item_repaid_time, 0) AS prev_order_item_repaid_time,
    COALESCE(pm.prev_jicai_order_item_repaid_time, 0) AS prev_jicai_order_item_repaid_time,
    COALESCE(pm.prev_non_jicai_order_item_repaid_time, 0) AS prev_non_jicai_order_item_repaid_time,
    COALESCE(a3m.avg_3m_sales_amount, 0) AS avg_3m_sales_amount,
    COALESCE(a3m.avg_3m_sales_gross_profit, 0) AS avg_3m_sales_gross_profit,
    COALESCE(a3m.avg_3m_settle_amount, 0) AS avg_3m_settle_amount,
    COALESCE(a3m.avg_3m_order_item_settle_time, 0) AS avg_3m_order_item_settle_time,
    COALESCE(a3m.avg_3m_jicai_order_item_settle_time, 0) AS avg_3m_jicai_order_item_settle_time,
    COALESCE(a3m.avg_3m_non_jicai_order_item_settle_time, 0) AS avg_3m_non_jicai_order_item_settle_time,
    COALESCE(a3m.avg_3m_repaid_amount, 0) AS avg_3m_repaid_amount,
    COALESCE(a3m.avg_3m_order_item_repaid_time, 0) AS avg_3m_order_item_repaid_time,
    COALESCE(a3m.avg_3m_jicai_order_item_repaid_time, 0) AS avg_3m_jicai_order_item_repaid_time,
    COALESCE(a3m.avg_3m_non_jicai_order_item_repaid_time, 0) AS avg_3m_non_jicai_order_item_repaid_time,
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
LEFT JOIN
    avg_3_months a3m 
    ON cm.stat_yearmonth = a3m.stat_yearmonth 
    AND cm.entryid = a3m.entryid
    AND cm.customid = a3m.customid
LEFT JOIN
    receivable_aging ra
    ON cm.stat_yearmonth = ra.stat_yearmonth
    AND cm.entryid = ra.entryid
    AND cm.customid = ra.customid;
