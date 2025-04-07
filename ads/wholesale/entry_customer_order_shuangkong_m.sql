DROP TABLE IF EXISTS ads.shuangkong_entry_m;
CREATE TABLE dws.wholesale_entry_customer_order_shuangkong_m (
    year_month DATE COMMENT '业务年月',
    entryid bigint COMMENT '独立单元ID',
    customid bigint COMMENT '客户ID',
    entry_name VARCHAR(255) COMMENT '独立单元名称',
    customer_name VARCHAR(255) COMMENT '客户名称',
    jicai_type VARCHAR(255) COMMENT '集采类型',
    nianbao_type VARCHAR(255),
    reputation_days INT,
    current_sales_amount DECIMAL(18,4),
    current_gross_profit_amount DECIMAL(18,4),
    current_payment_amount DECIMAL(18,4),
    current_payment_days DECIMAL(18,4),
    prev_sales_amount DECIMAL(18,4),
    prev_gross_profit_amount DECIMAL(18,4),
    prev_payment_amount DECIMAL(18,4),
    prev_payment_days DECIMAL(18,4),
    avg_3m_sales_amount DECIMAL(18,4),
    avg_3m_gross_profit_amount DECIMAL(18,4),
    avg_3m_payment_amount DECIMAL(18,4),
    avg_3m_payment_days DECIMAL(18,4),
    avg_3m_payment_rate DECIMAL(18,4),
    avg_3m_payment_overdue_rate DECIMAL(18,4)
)
UNIQUE KEY(year_month, entryid, customid) 
DISTRIBUTED BY HASH(year_month, entryid, customid) 
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 步骤1：创建销售数据临时表
DROP TABLE IF EXISTS temp_sales_data;
CREATE TABLE temp_sales_data AS
SELECT 
    t1.salesid,
    t1.salesdtlid,
    t1.entryid,
    t1.entry_name,
    t1.customid,
    t1.customer_name,
    t1.jicai_type,
    t1.nianbao_type,
    CASE
        WHEN t1.nianbao_type = '医疗器械' THEN confirm_date
        ELSE t1.create_date
    END as create_date,
    t1.sales_amount,
    t1.batch_gross_profit,
    t2.reputation_days
FROM
    dwd.wholesale_order_sales_dtl t1
JOIN dim.entry_customer_xinyu t2
  ON t1.customid = t2.customid
WHERE t1.use_status = '正式' 
  AND t1.credit_approve_status != '审批拒绝' 
  AND t1.is_btob = 0
  AND t1.create_date >= t2.dw_starttime AND t1.create_date < t2.dw_endtime;

-- 步骤2：创建支付数据临时表
DROP TABLE IF EXISTS temp_payment_data;
CREATE TABLE temp_payment_data AS
SELECT
    sa.salesid,
    sa.salesdtlid,
    se.sasettledtlid,
    pa.sarecdtlid,
    pa.payment_date,
    sa.entryid,
    sa.customid,
    sa.jicai_type,
    sa.nianbao_type,
    sa.reputation_days,
    datediff(pa.payment_date, sa.create_date) as payment_days,
    pa.payment_amount
FROM temp_sales_data sa
JOIN dwd.wholesale_order_settle_dtl se ON sa.salesdtlid = se.salesdtlid
JOIN dwd.wholesale_order_repay_dtl pa ON se.sasettledtlid = pa.sasettledtlid
WHERE pa.use_status = '正式' and pa.is_btob = 0;

-- 步骤3：创建月度销售数据临时表
DROP TABLE IF EXISTS temp_sales_monthly_data;
CREATE TABLE temp_sales_monthly_data AS
SELECT 
    DATE_FORMAT(create_date, '%Y-%m') AS year_month,
    entryid,
    entry_name,
    customid,
    customer_name,
    jicai_type,
    nianbao_type,
    reputation_days,
    SUM(sales_amount) AS total_amount,
    SUM(batch_gross_profit) AS total_profit
FROM 
    temp_sales_data
GROUP BY 
    DATE_FORMAT(create_date, '%Y-%m'), entryid, entry_name, customid, customer_name, jicai_type, nianbao_type, reputation_days;

-- 步骤4：创建月度支付数据临时表
DROP TABLE IF EXISTS temp_payment_monthly_data;
CREATE TABLE temp_payment_monthly_data AS
SELECT 
    DATE_FORMAT(payment_date, '%Y-%m') AS year_month,
    entryid,
    customid,
    jicai_type,
    nianbao_type,
    reputation_days,
    SUM(payment_amount) AS total_payment,
    AVG(payment_days) AS avg_payment_days
FROM 
    temp_payment_data
GROUP BY 
    DATE_FORMAT(payment_date, '%Y-%m'), entryid, customid, jicai_type, nianbao_type, reputation_days;

-- 步骤5：创建完整月份临时表
DROP TABLE IF EXISTS temp_complete_months;
CREATE TABLE temp_complete_months AS
WITH date_data AS (
    SELECT distinct DATE_FORMAT(date_key, '%Y-%m') AS year_month
    FROM dim.date
    WHERE date_key < CURRENT_DATE()
    ORDER by year_month
),
customer_units1 AS (
    SELECT DISTINCT entryid, customid, jicai_type, nianbao_type, reputation_days
    FROM temp_sales_monthly_data
),
customer_units2 AS (
    SELECT DISTINCT entryid, customid, jicai_type, nianbao_type, reputation_days
    FROM temp_payment_monthly_data
),
-- 合并customer_units1和customer_units2的所有可能组合
combined_units AS (
    SELECT DISTINCT 
        COALESCE(cu1.entryid, cu2.entryid) as entryid,
        COALESCE(cu1.customid, cu2.customid) as customid,
        cu1.jicai_type as jicai_type1,
        cu2.jicai_type as jicai_type2,
        cu1.nianbao_type as nianbao_type1,
        cu2.nianbao_type as nianbao_type2,
        cu1.reputation_days as reputation_days1,
        cu2.reputation_days as reputation_days2
    FROM customer_units1 cu1
    FULL OUTER JOIN customer_units2 cu2 
    ON cu1.entryid = cu2.entryid 
    AND cu1.customid = cu2.customid
)
SELECT
    dd.year_month,
    cu.entryid,
    e.entry_name,
    cu.customid,
    c.customer_name,
    COALESCE(cu.jicai_type1, cu.jicai_type2) as jicai_type,
    COALESCE(cu.nianbao_type1, cu.nianbao_type2) as nianbao_type,
    COALESCE(cu.reputation_days1, cu.reputation_days2) as reputation_days
FROM date_data dd
CROSS JOIN combined_units cu
LEFT JOIN (
    SELECT DISTINCT entryid, entry_name 
    FROM temp_sales_monthly_data
) e ON cu.entryid = e.entryid
LEFT JOIN (
    SELECT DISTINCT customid, customer_name 
    FROM temp_sales_monthly_data
) c ON cu.customid = c.customid;

-- 步骤6：创建合并月度数据临时表
DROP TABLE IF EXISTS temp_combined_monthly_data;
CREATE TABLE temp_combined_monthly_data AS
SELECT 
    cm.year_month,
    cm.entryid,
    cm.entry_name,
    cm.customid,
    cm.customer_name,
    cm.jicai_type,
    cm.nianbao_type,
    cm.reputation_days,
    COALESCE(s.total_amount, 0) AS total_amount,
    COALESCE(s.total_profit, 0) AS total_profit,
    COALESCE(p.total_payment, 0) AS total_payment,
    COALESCE(p.avg_payment_days, 0) AS avg_payment_days
FROM 
    temp_complete_months cm
LEFT JOIN
    temp_sales_monthly_data s ON cm.year_month = s.year_month
                             AND cm.entryid = s.entryid 
                             AND cm.customid = s.customid
                             AND cm.jicai_type = s.jicai_type
                             AND cm.nianbao_type = s.nianbao_type
                             AND cm.reputation_days = s.reputation_days
LEFT JOIN 
    temp_payment_monthly_data p ON cm.year_month = p.year_month 
                              AND cm.entryid = p.entryid 
                              AND cm.customid = p.customid
                              AND cm.jicai_type = p.jicai_type
                              AND cm.nianbao_type = p.nianbao_type
                              AND cm.reputation_days = p.reputation_days;

-- 步骤7：创建历史数据临时表
DROP TABLE IF EXISTS temp_historical_data;
CREATE TABLE temp_historical_data AS
SELECT 
    m1.year_month,
    m1.entryid,
    m1.entry_name,
    m1.customid,
    m1.customer_name,
    m1.jicai_type,
    m1.nianbao_type,
    m1.reputation_days,
    -- Current month metrics
    m1.total_amount AS current_sales_amount,
    m1.total_profit AS current_gross_profit_amount,
    m1.total_payment AS current_payment_amount,
    m1.avg_payment_days AS current_payment_days,
    
    -- Last month metrics
    COALESCE(m2.total_amount, 0) AS prev_sales_amount,
    COALESCE(m2.total_profit, 0) AS prev_gross_profit_amount,
    COALESCE(m2.total_payment, 0) AS prev_payment_amount,
    COALESCE(m2.avg_payment_days, 0) AS prev_payment_days,
    
    -- 3-month average amount
    COALESCE((COALESCE(m2.total_amount, 0) + COALESCE(m3.total_amount, 0) + COALESCE(m4.total_amount, 0)) / 3, 0) AS avg_3m_sales_amount,
    
    -- 3-month average profit
    COALESCE((COALESCE(m2.total_profit, 0) + COALESCE(m3.total_profit, 0) + COALESCE(m4.total_profit, 0)) / 3, 0) AS avg_3m_gross_profit_amount,
    
    -- 3-month average payment
    COALESCE((COALESCE(m2.total_payment, 0) + COALESCE(m3.total_payment, 0) + COALESCE(m4.total_payment, 0)) / 3, 0) AS avg_3m_payment_amount,

    -- 3-month average payment days
    COALESCE((COALESCE(m2.avg_payment_days, 0) + COALESCE(m3.avg_payment_days, 0) + COALESCE(m4.avg_payment_days, 0)) / 3, 0) AS avg_3m_payment_days
FROM 
    temp_combined_monthly_data m1
LEFT JOIN 
    temp_combined_monthly_data m2 ON m1.entryid = m2.entryid  
                                AND m1.customid = m2.customid
                                AND m1.jicai_type = m2.jicai_type
                                AND m1.nianbao_type = m2.nianbao_type
                                AND m1.reputation_days = m2.reputation_days
                                AND m2.year_month = DATE_FORMAT(DATE_SUB(STR_TO_DATE(CONCAT(m1.year_month, '-01'), '%Y-%m-%d'), INTERVAL 1 MONTH), '%Y-%m')
LEFT JOIN 
    temp_combined_monthly_data m3 ON m1.entryid = m3.entryid 
                                AND m1.customid = m3.customid
                                AND m1.jicai_type = m3.jicai_type
                                AND m1.nianbao_type = m3.nianbao_type
                                AND m1.reputation_days = m3.reputation_days
                                AND m3.year_month = DATE_FORMAT(DATE_SUB(STR_TO_DATE(CONCAT(m1.year_month, '-01'), '%Y-%m-%d'), INTERVAL 2 MONTH), '%Y-%m')
LEFT JOIN 
    temp_combined_monthly_data m4 ON m1.entryid = m4.entryid 
                                AND m1.customid = m4.customid
                                AND m1.jicai_type = m4.jicai_type
                                AND m1.nianbao_type = m4.nianbao_type
                                AND m1.reputation_days = m4.reputation_days
                                AND m4.year_month = DATE_FORMAT(DATE_SUB(STR_TO_DATE(CONCAT(m1.year_month, '-01'), '%Y-%m-%d'), INTERVAL 3 MONTH), '%Y-%m');

-- 步骤8：最终插入目标表
INSERT INTO dws.wholesale_entry_customer_order_shuangkong_m
SELECT 
    STR_TO_DATE(year_month, '%Y-%m') as year_month,
    entryid,
    entry_name,
    customid,
    customer_name,
    jicai_type,
    nianbao_type,
    reputation_days,
    ROUND(current_sales_amount, 2) as current_sales_amount,
    ROUND(current_gross_profit_amount, 2) as current_gross_profit_amount,
    ROUND(current_payment_amount, 2) as current_payment_amount,
    ROUND(current_payment_days, 2) as current_payment_days,
    ROUND(prev_sales_amount, 2) as prev_sales_amount,
    ROUND(prev_gross_profit_amount, 2) as prev_gross_profit_amount,
    ROUND(prev_payment_amount, 2) as prev_payment_amount,
    ROUND(prev_payment_days, 2) as prev_payment_days,
    ROUND(avg_3m_sales_amount, 2) as avg_3m_sales_amount,
    ROUND(avg_3m_gross_profit_amount, 2) as avg_3m_gross_profit_amount,
    ROUND(avg_3m_payment_amount, 2) as avg_3m_payment_amount,
    ROUND(avg_3m_payment_days, 2) as avg_3m_payment_days,
    CASE
        WHEN avg_3m_sales_amount > 0 AND avg_3m_payment_amount = 0 THEN -1
        WHEN avg_3m_sales_amount = 0 AND avg_3m_payment_amount = 0 THEN 0
        WHEN avg_3m_sales_amount = 0 AND avg_3m_payment_amount > 0 THEN 1
        ELSE ROUND(avg_3m_payment_amount / avg_3m_sales_amount, 2)
    END AS avg_3m_payment_rate,
    CASE
        WHEN avg_3m_payment_days = 0 AND reputation_days > 0 THEN -1
        WHEN avg_3m_payment_days >= 0 AND reputation_days = 0 THEN 0
        ELSE ROUND(avg_3m_payment_days / reputation_days, 2)
    END AS avg_3m_payment_overdue_rate
FROM 
    temp_historical_data
WHERE current_sales_amount<>0 or current_gross_profit_amount <> 0 or current_payment_amount <> 0
    OR prev_sales_amount<>0 or prev_gross_profit_amount <> 0 or prev_payment_amount <> 0
    OR avg_3m_sales_amount<>0 or avg_3m_gross_profit_amount <> 0 or avg_3m_payment_amount <> 0
ORDER BY 
    year_month, entryid, customid;

-- 清理临时表（可选，如果需要保留临时表用于调试，可以注释掉这部分）
DROP TABLE IF EXISTS temp_sales_data;
DROP TABLE IF EXISTS temp_payment_data;
DROP TABLE IF EXISTS temp_sales_monthly_data;
DROP TABLE IF EXISTS temp_payment_monthly_data;
DROP TABLE IF EXISTS temp_date_data;
DROP TABLE IF EXISTS temp_customer_units;
DROP TABLE IF EXISTS temp_complete_months;
DROP TABLE IF EXISTS temp_combined_monthly_data;
DROP TABLE IF EXISTS temp_historical_data;