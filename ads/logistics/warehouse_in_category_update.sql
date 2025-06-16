
INSERT INTO
    ads.logistics_warehouse_in_category_m (
        stat_yearmonth,
        warehid,
        category,
        warehouse_name,
        median_time_receive_to_check,
        median_time_check_to_flat,
        median_time_check_to_auto,
        median_time_receive_to_flat,
        median_time_receive_to_auto,
        median_working_time_receive_to_check,
        median_working_time_check_to_flat,
        median_working_time_check_to_auto,
        median_working_time_receive_to_flat,
        median_working_time_receive_to_auto,
        p95_time_receive_to_check,
        p95_time_check_to_flat,
        p95_time_check_to_auto,
        p95_time_receive_to_flat,
        p95_time_receive_to_auto,
        p95_working_time_receive_to_check,
        p95_working_time_check_to_flat,
        p95_working_time_check_to_auto,
        p95_working_time_receive_to_flat,
        p95_working_time_receive_to_auto,
        h24_finish_rate_receive_to_check,
        h24_finish_rate_check_to_flat,
        h24_finish_rate_check_to_auto,
        h24_finish_rate_receive_to_flat,
        h24_finish_rate_receive_to_auto,
        h48_finish_rate_receive_to_flat,
        h48_finish_rate_receive_to_auto
    )
WITH
    base_data AS (
        SELECT
            warehid,
            warehouse_name,
            -- 根据 is_coldchain 和 is_chinese_medicine 确定类别
            CASE 
                WHEN is_coldchain = 1 THEN '冷链'
                WHEN is_chinese_medicine = 1 THEN '中药'
                ELSE '其他'
            END as category,
            -- 根据收货时间确定统计月份
            CASE
                WHEN receive_time IS NOT NULL THEN DATE_TRUNC(receive_time, 'MONTH')
                WHEN check_time IS NOT NULL THEN DATE_TRUNC(check_time, 'MONTH')
                WHEN finish_time IS NOT NULL THEN DATE_TRUNC(finish_time, 'MONTH')
                WHEN iwcs_finish_time IS NOT NULL THEN DATE_TRUNC(iwcs_finish_time, 'MONTH')
            END as stat_date,
            -- 时间指标（转换为小时）
            time_receive_to_check / 60.0 as time_receive_to_check_hours,
            time_check_to_flat / 60.0 as time_check_to_flat_hours,
            time_check_to_auto / 60.0 as time_check_to_auto_hours,
            time_receive_to_flat / 60.0 as time_receive_to_flat_hours,
            time_receive_to_auto / 60.0 as time_receive_to_auto_hours,
            -- 工作时间指标（转换为小时）
            working_time_receive_to_check / 60.0 as working_time_receive_to_check_hours,
            working_time_check_to_flat / 60.0 as working_time_check_to_flat_hours,
            working_time_check_to_auto / 60.0 as working_time_check_to_auto_hours,
            working_time_receive_to_flat / 60.0 as working_time_receive_to_flat_hours,
            working_time_receive_to_auto / 60.0 as working_time_receive_to_auto_hours,
            -- 24小时完成率标识
            CASE WHEN time_receive_to_check <= 1440 THEN 1 ELSE 0 END as h24_receive_to_check,
            CASE WHEN time_check_to_flat <= 1440 THEN 1 ELSE 0 END as h24_check_to_flat,
            CASE WHEN time_check_to_auto <= 1440 THEN 1 ELSE 0 END as h24_check_to_auto,
            CASE WHEN time_receive_to_flat <= 1440 THEN 1 ELSE 0 END as h24_receive_to_flat,
            CASE WHEN time_receive_to_auto <= 1440 THEN 1 ELSE 0 END as h24_receive_to_auto,
            -- 48小时完成率标识
            CASE WHEN time_receive_to_flat <= 2880 THEN 1 ELSE 0 END as h48_receive_to_flat,
            CASE WHEN time_receive_to_auto <= 2880 THEN 1 ELSE 0 END as h48_receive_to_auto
        FROM
            dwd.logistics_warehouse_in_time
        WHERE
            is_autotask = 0
    )
-- 具体类别统计
SELECT
    stat_date as stat_yearmonth,
    warehid,
    category,
    warehouse_name,
    PERCENTILE(time_receive_to_check_hours, 0.5) as median_time_receive_to_check,
    PERCENTILE(time_check_to_flat_hours, 0.5) as median_time_check_to_flat,
    PERCENTILE(time_check_to_auto_hours, 0.5) as median_time_check_to_auto,
    PERCENTILE(time_receive_to_flat_hours, 0.5) as median_time_receive_to_flat,
    PERCENTILE(time_receive_to_auto_hours, 0.5) as median_time_receive_to_auto,
    PERCENTILE(working_time_receive_to_check_hours, 0.5) as median_working_time_receive_to_check,
    PERCENTILE(working_time_check_to_flat_hours, 0.5) as median_working_time_check_to_flat,
    PERCENTILE(working_time_check_to_auto_hours, 0.5) as median_working_time_check_to_auto,
    PERCENTILE(working_time_receive_to_flat_hours, 0.5) as median_working_time_receive_to_flat,
    PERCENTILE(working_time_receive_to_auto_hours, 0.5) as median_working_time_receive_to_auto,
    PERCENTILE(time_receive_to_check_hours, 0.95) as p95_time_receive_to_check,
    PERCENTILE(time_check_to_flat_hours, 0.95) as p95_time_check_to_flat,
    PERCENTILE(time_check_to_auto_hours, 0.95) as p95_time_check_to_auto,
    PERCENTILE(time_receive_to_flat_hours, 0.95) as p95_time_receive_to_flat,
    PERCENTILE(time_receive_to_auto_hours, 0.95) as p95_time_receive_to_auto,
    PERCENTILE(working_time_receive_to_check_hours, 0.95) as p95_working_time_receive_to_check,
    PERCENTILE(working_time_check_to_flat_hours, 0.95) as p95_working_time_check_to_flat,
    PERCENTILE(working_time_check_to_auto_hours, 0.95) as p95_working_time_check_to_auto,
    PERCENTILE(working_time_receive_to_flat_hours, 0.95) as p95_working_time_receive_to_flat,
    PERCENTILE(working_time_receive_to_auto_hours, 0.95) as p95_working_time_receive_to_auto,
    -- 24小时完成率
    AVG(h24_receive_to_check) as h24_finish_rate_receive_to_check,
    AVG(h24_check_to_flat) as h24_finish_rate_check_to_flat,
    AVG(h24_check_to_auto) as h24_finish_rate_check_to_auto,
    AVG(h24_receive_to_flat) as h24_finish_rate_receive_to_flat,
    AVG(h24_receive_to_auto) as h24_finish_rate_receive_to_auto,
    -- 48小时完成率
    AVG(h48_receive_to_flat) as h48_finish_rate_receive_to_flat,
    AVG(h48_receive_to_auto) as h48_finish_rate_receive_to_auto
FROM
    base_data
WHERE
    DATE_FORMAT(stat_date, '%Y%m') IN (
        DATE_FORMAT(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 1 MONTH), '%Y%m'),
        DATE_FORMAT(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 2 MONTH), '%Y%m')
    )
GROUP BY
    stat_date,
    warehid,
    category,
    warehouse_name

UNION ALL

-- 所有类别汇总统计
SELECT
    stat_date as stat_yearmonth,
    warehid,
    '所有' as category,
    warehouse_name,
    PERCENTILE(time_receive_to_check_hours, 0.5) as median_time_receive_to_check,
    PERCENTILE(time_check_to_flat_hours, 0.5) as median_time_check_to_flat,
    PERCENTILE(time_check_to_auto_hours, 0.5) as median_time_check_to_auto,
    PERCENTILE(time_receive_to_flat_hours, 0.5) as median_time_receive_to_flat,
    PERCENTILE(time_receive_to_auto_hours, 0.5) as median_time_receive_to_auto,
    PERCENTILE(working_time_receive_to_check_hours, 0.5) as median_working_time_receive_to_check,
    PERCENTILE(working_time_check_to_flat_hours, 0.5) as median_working_time_check_to_flat,
    PERCENTILE(working_time_check_to_auto_hours, 0.5) as median_working_time_check_to_auto,
    PERCENTILE(working_time_receive_to_flat_hours, 0.5) as median_working_time_receive_to_flat,
    PERCENTILE(working_time_receive_to_auto_hours, 0.5) as median_working_time_receive_to_auto,
    PERCENTILE(time_receive_to_check_hours, 0.95) as p95_time_receive_to_check,
    PERCENTILE(time_check_to_flat_hours, 0.95) as p95_time_check_to_flat,
    PERCENTILE(time_check_to_auto_hours, 0.95) as p95_time_check_to_auto,
    PERCENTILE(time_receive_to_flat_hours, 0.95) as p95_time_receive_to_flat,
    PERCENTILE(time_receive_to_auto_hours, 0.95) as p95_time_receive_to_auto,
    PERCENTILE(working_time_receive_to_check_hours, 0.95) as p95_working_time_receive_to_check,
    PERCENTILE(working_time_check_to_flat_hours, 0.95) as p95_working_time_check_to_flat,
    PERCENTILE(working_time_check_to_auto_hours, 0.95) as p95_working_time_check_to_auto,
    PERCENTILE(working_time_receive_to_flat_hours, 0.95) as p95_working_time_receive_to_flat,
    PERCENTILE(working_time_receive_to_auto_hours, 0.95) as p95_working_time_receive_to_auto,
    -- 24小时完成率
    AVG(h24_receive_to_check) as h24_finish_rate_receive_to_check,
    AVG(h24_check_to_flat) as h24_finish_rate_check_to_flat,
    AVG(h24_check_to_auto) as h24_finish_rate_check_to_auto,
    AVG(h24_receive_to_flat) as h24_finish_rate_receive_to_flat,
    AVG(h24_receive_to_auto) as h24_finish_rate_receive_to_auto,
    -- 48小时完成率
    AVG(h48_receive_to_flat) as h48_finish_rate_receive_to_flat,
    AVG(h48_receive_to_auto) as h48_finish_rate_receive_to_auto
FROM
    base_data
WHERE
    DATE_FORMAT(stat_date, '%Y%m') IN (
        DATE_FORMAT(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), '%Y%m'),
        DATE_FORMAT(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 1 MONTH), '%Y%m'),
        DATE_FORMAT(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 2 MONTH), '%Y%m')
    )
GROUP BY
    stat_date,
    warehid,
    warehouse_name;