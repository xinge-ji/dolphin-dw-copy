INSERT INTO
    ads.logistics_warehouse_in_operationtype_d (
        stat_date,
        warehid,
        goodsownerid,
        operationtype,
        warehouse_name,
        goodsowner_name,
        median_time_order_to_receive,
        p95_time_order_to_receive
    )
WITH
    base_data AS (
        SELECT
            warehid,
            goodsownerid,
            operation_type,
            warehouse_name,
            goodsowner_name,
            -- 订单到收货相关指标 (stat_date = DATE(receive_time))
            CASE
                WHEN receive_time IS NOT NULL THEN DATE_TRUNC (receive_time, 'DAY')
            END as stat_date_receive,
            time_order_to_receive / 1440.0 as time_order_to_receive_days
        FROM
            dwd.logistics_warehouse_in_time
        WHERE
            is_autotask = 0
            AND receive_time >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 2 MONTH)
    )
-- 原始数据：具体货主 + 具体业务类型
SELECT
    stat_date_receive as stat_yearmonth,
    warehid,
    goodsownerid,
    operation_type,
    warehouse_name,
    goodsowner_name,
    PERCENTILE (time_order_to_receive_days, 0.5) as median_time_order_to_receive,
    PERCENTILE (time_order_to_receive_days, 0.95) as p95_time_order_to_receive
FROM
    base_data
GROUP BY
    stat_date_receive,
    warehid,
    goodsownerid,
    operation_type,
    warehouse_name,
    goodsowner_name

UNION ALL

-- 货主为所有 + 各个业务类型
SELECT
    stat_date_receive as stat_yearmonth,
    warehid,
    -1 as goodsownerid,  -- 使用 -1 表示所有货主
    operation_type,
    warehouse_name,
    '所有货主' as goodsowner_name,
    PERCENTILE (time_order_to_receive_days, 0.5) as median_time_order_to_receive,
    PERCENTILE (time_order_to_receive_days, 0.95) as p95_time_order_to_receive
FROM
    base_data
GROUP BY
    stat_date_receive,
    warehid,
    operation_type,
    warehouse_name

UNION ALL

-- 货主为所有 + 业务类型为所有
SELECT
    stat_date_receive as stat_yearmonth,
    warehid,
    -1 as goodsownerid,  -- 使用 -1 表示所有货主
    '所有业务类型' as operationtype,
    warehouse_name,
    '所有货主' as goodsowner_name,
    PERCENTILE (time_order_to_receive_days, 0.5) as median_time_order_to_receive,
    PERCENTILE (time_order_to_receive_days, 0.95) as p95_time_order_to_receive
FROM
    base_data
GROUP BY
    stat_date_receive,
    warehid,
    warehouse_name;