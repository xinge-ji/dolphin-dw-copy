DROP TABLE IF EXISTS ads.logistics_warehouse_in_operationtype_d;

CREATE TABLE
    ads.logistics_warehouse_in_operationtype_d (
        -- 颗粒度
        stat_date date COMMENT '统计日期',
        warehid bigint COMMENT '仓库ID',
        goodsownerid bigint COMMENT '货主ID',
        operationtype varchar COMMENT '业务类型',
        -- 描述
        warehouse_name varchar COMMENT '仓库名称',
        goodsowner_name varchar COMMENT '货主名称',
        -- 指标
        median_time_order_to_receive decimal COMMENT '订单到收货天数中位数',
        p95_time_order_to_receive decimal COMMENT '订单到收货天数95分位数'
    ) UNIQUE KEY (
        stat_date,
        warehid,
        goodsownerid,
        operationtype
    ) DISTRIBUTED BY HASH (
        stat_date,
        warehid,
        goodsownerid,
        operationtype
    ) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

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
