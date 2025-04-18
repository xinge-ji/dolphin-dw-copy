INSERT INTO dws.wholesale_approval_d (
    stat_date,
    entryid,
    customid,
    entry_name,
    province_name,
    customer_name,
    customertype_name,
    customertype_group,
    order_count,
    approved_order_count,
    rejected_order_count,
    approval_node_count,
    avg_node_approval_time
)
WITH date_range AS (
    -- 生成日期序列辅助表
    SELECT n AS day_offset
    FROM (
        SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL 
        SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL 
        SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL 
        SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL 
        SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14
    ) t -- 支持最多14天的审批周期
),
-- 生成从通知日期到审批日期之间的所有日期
date_series AS (
    SELECT 
        a.salesid,
        a.entryid,
        a.customid,
        a.entry_name,
        a.approval_node,
        a.notify_time,
        a.approval_time,
        DATE_ADD(DATE(a.notify_time), INTERVAL dr.day_offset DAY) AS work_date
    FROM dwd.wholesale_order_approval a
    JOIN date_range dr
    WHERE a.submit_time IS NOT NULL 
      AND a.notify_time IS NOT NULL
      AND a.approval_time IS NOT NULL
      AND a.approval_comment NOT LIKE '%系统自动%'
      AND DATE_ADD(DATE(a.notify_time), INTERVAL dr.day_offset DAY) <= DATE(a.approval_time)
      AND a.approval_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
),
-- 计算每个日期的工作时间
work_time_calc AS (
    SELECT
        ds.salesid,
        ds.entryid,
        ds.customid,
        ds.entry_name,
        ds.approval_node,
        ds.work_date,
        -- 排除周末 (1=周日, 7=周六)
        CASE WHEN DAYOFWEEK(ds.work_date) IN (1, 7) THEN 0
        ELSE 
            -- 计算当天有效工作分钟数
            TIMESTAMPDIFF(
                MINUTE,
                -- 当天开始工作时间
                GREATEST(
                    STR_TO_DATE(CONCAT(ds.work_date, ' 08:00:00'), 'yyyy-MM-dd HH:mm:ss'),
                    CASE 
                        WHEN ds.work_date = DATE(ds.notify_time) THEN
                            CASE 
                                WHEN EXTRACT(hour FROM ds.notify_time) < 8 THEN STR_TO_DATE(CONCAT(ds.work_date, ' 08:00:00'), 'yyyy-MM-dd HH:mm:ss')
                                WHEN EXTRACT(hour FROM ds.notify_time) > 20 THEN STR_TO_DATE(CONCAT(DATE_ADD(ds.work_date, INTERVAL 1 DAY), ' 20:00:00'), 'yyyy-MM-dd HH:mm:ss')
                                ELSE ds.notify_time
                            END
                        ELSE STR_TO_DATE(CONCAT(ds.work_date, ' 08:00:00'), 'yyyy-MM-dd HH:mm:ss')
                    END
                ),
                -- 当天结束工作时间
                LEAST(
                    STR_TO_DATE(CONCAT(ds.work_date, ' 20:00:00'), 'yyyy-MM-dd HH:mm:ss'),
                    CASE 
                        WHEN ds.work_date = DATE(ds.approval_time) THEN
                            CASE 
                                WHEN EXTRACT(hour FROM ds.approval_time) < 8 THEN STR_TO_DATE(CONCAT(ds.work_date, ' 08:00:00'), 'yyyy-MM-dd HH:mm:ss')
                                WHEN EXTRACT(hour FROM ds.approval_time) > 20 THEN STR_TO_DATE(CONCAT(ds.work_date, ' 20:00:00'), 'yyyy-MM-dd HH:mm:ss')
                                ELSE ds.approval_time
                            END
                        ELSE STR_TO_DATE(CONCAT(ds.work_date, ' 20:00:00'), 'yyyy-MM-dd HH:mm:ss')
                    END
                )
            )
        END AS daily_minutes
    FROM date_series ds
),
-- 按审批节点汇总审批时长
approval_node_data AS (
    SELECT
        DATE(a.approval_time) AS stat_date,
        a.entryid,
        a.customid,
        a.salesid,
        a.entry_name,
        a.approval_node,
        SUM(wtc.daily_minutes) AS approval_duration_minutes
    FROM dwd.wholesale_order_approval a
    LEFT JOIN work_time_calc wtc ON a.salesid = wtc.salesid AND a.approval_node = wtc.approval_node
    WHERE a.submit_time IS NOT NULL 
      AND a.notify_time IS NOT NULL
      AND a.approval_time IS NOT NULL
      AND a.approval_comment NOT LIKE '%系统自动%'
      AND a.approval_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    GROUP BY
        DATE(a.approval_time),
        a.entryid,
        a.customid,
        a.salesid,
        a.entry_name,
        a.approval_node
),

-- 获取每个订单最新的审批记录
latest_approval AS (
    SELECT 
        DATE(a.approval_time) AS stat_date,
        a.salesid,
        a.entryid,
        a.customid,
        CASE 
            WHEN a.approval_comment LIKE '%驳回%' THEN 0
            ELSE 1
        END AS is_approved
    FROM dwd.wholesale_order_approval a
    INNER JOIN (
        -- 获取每个订单的最新审批时间
        SELECT 
            salesid, 
            MAX(approval_time) AS latest_approval_time
        FROM dwd.wholesale_order_approval
        WHERE approval_time IS NOT NULL
          AND approval_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        GROUP BY salesid
    ) latest ON a.salesid = latest.salesid AND a.approval_time = latest.latest_approval_time
),
-- 合并审批节点数据和客户维度数据
approval_data AS (
    SELECT
        n.stat_date,
        n.entryid,
        n.customid,
        n.salesid,
        n.entry_name,
        c.province_name,
        c.customer_name,
        c.customertype_name,
        c.customertype_group,
        n.approval_duration_minutes,
        n.approval_node,
        l.is_approved
    FROM approval_node_data n
    JOIN latest_approval l ON n.salesid = l.salesid
    JOIN dwd.wholesale_order_sales_doc c ON n.entryid = c.entryid AND n.customid = c.customid AND n.salesid = c.salesid
)
SELECT
    stat_date,
    entryid,
    customid,
    MAX(entry_name) AS entry_name,
    MAX(province_name) AS province_name,
    MAX(customer_name) AS customer_name,
    MAX(customertype_name) AS customertype_name,
    MAX(customertype_group) AS customertype_group,
    COUNT(DISTINCT salesid) AS order_count,
    COUNT(DISTINCT CASE WHEN is_approved = 1 THEN salesid END) AS approved_order_count,
    COUNT(DISTINCT CASE WHEN is_approved = 0 THEN salesid END) AS rejected_order_count,
    COUNT(approval_node) AS approval_node_count,
    ROUND(AVG(CASE WHEN approval_duration_minutes > 0 THEN approval_duration_minutes ELSE NULL END)) AS avg_node_approval_time
FROM approval_data
GROUP BY 
    stat_date,
    entryid,
    customid;