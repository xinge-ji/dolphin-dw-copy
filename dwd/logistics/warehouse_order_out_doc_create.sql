DROP TABLE IF EXISTS dwd.logistics_warehouse_order_out_doc;

CREATE TABLE
    dwd.logistics_warehouse_order_out_doc (
        -- 主键标识
        outid bigint COMMENT '出库订单总单ID',

        -- 数据更新时间
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 关联单据
        goodsownerid bigint COMMENT '货主ID',
        warehid bigint COMMENT '仓库ID',
        waveid bigint COMMENT '波次ID',
        wavedtlid bigint COMMENT '波次明细ID',
        togoodsownerid bigint COMMENT '目标货主ID',

        -- 时间
        create_time DATETIME COMMENT '下单时间',

        -- 其他
        is_eshop tinyint COMMENT '是否云商订单',
        is_autopass tinyint COMMENT '是否自动转单',
        is_passing tinyint COMMENT '是否转单',
        use_status varchar COMMENT '订单状态',
        operation_type varchar COMMENT '业务类型'
    ) UNIQUE KEY (outid) DISTRIBUTED BY HASH (outid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO dwd.logistics_warehouse_order_out_doc (
    outid,
    dw_updatetime,
    goodsownerid,
    warehid,
    waveid,
    wavedtlid,
    togoodsownerid,
    create_time,
    is_eshop,
    is_autopass,
    is_passing,
    use_status,
    operation_type
)
SELECT 
    a.outid,
    a.dw_updatetime,
    a.goodsownerid,
    a.warehid,
    a.waveid,
    a.wavedtlid,
    a.togoodsownerid,
    a.credate as create_time,
    IFNULL(a.ysorderflag, 0) as is_eshop,
    IFNULL(a.autopassflag, 0) as is_autopass,
    IFNULL(a.passingflag, 0) as is_passing,
    CASE 
        WHEN a.usestatus = -1 THEN '库存不够'
        WHEN a.usestatus = 0 THEN '取消'
        WHEN a.usestatus = 1 THEN '下单'
        WHEN a.usestatus = 2 THEN '待拣货'
        WHEN a.usestatus = 3 THEN '待复核'
        WHEN a.usestatus = 4 THEN '已出货'
        WHEN a.usestatus = 5 THEN '已抵达'
        WHEN a.usestatus = 6 THEN '等待补货'
        WHEN a.usestatus = 7 THEN '出库取消'
        ELSE '其他'
    END as use_status,
    s.ddlname as operation_type
FROM ods_wms.wms_out_order a
LEFT JOIN ods_wms.sys_ddl_dtl s ON a.operationtype = s.ddlid AND s.sysid = 389 AND s.is_active = 1
WHERE a.is_active = 1;