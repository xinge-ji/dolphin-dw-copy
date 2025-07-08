DROP TABLE IF EXISTS dwd.logistics_tms_order_doc;

CREATE TABLE
    dwd.logistics_tms_order_doc (
        -- 主键标识
        orderid bigint COMMENT '订单ID',

        -- 时间
        dw_updatetime datetime COMMENT '数据更新时间',
        create_time datetime COMMENT '建立时间',
        create_order_time datetime COMMENT '创建订单时间',
        load_time datetime COMMENT '出库时间',
        sign_time datetime COMMENT '签收时间',

        -- 随货同行单
        goodspeerno bigint COMMENT '随货同行单号'
    ) UNIQUE KEY (orderid) DISTRIBUTED BY HASH (orderid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO dwd.logistics_tms_order_doc (
    orderid,
    dw_updatetime,
    create_time,
    create_order_time,
    load_time,
    sign_time,
    goodspeerno
)
SELECT
    orderid,
    dw_updatetime,
    createdtime AS create_time,
    createOrderTime AS create_order_time,
    loadtime AS load_time,
    signtime AS sign_time,
    goodspeerno
FROM
    ods_tms.mlogisticsorder
WHERE is_active = 1

UNION

SELECT
    orderid,
    dw_updatetime,
    createdtime AS create_time,
    createOrderTime AS create_order_time,
    loadtime AS load_time,
    signtime AS sign_time,
    goodspeerno
FROM
    ods_tms.mlogisticsorderhistory
WHERE is_active = 1;