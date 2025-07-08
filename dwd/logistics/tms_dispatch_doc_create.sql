DROP TABLE IF EXISTS dwd.logistics_tms_dispatch_doc;

CREATE TABLE
    dwd.logistics_tms_dispatch_doc (
        -- 主键标识
        dispatchid bigint COMMENT '调度单ID',
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 时间
        create_time datetime COMMENT '建立时间',

        -- 仓库
        warehid bigint COMMENT '仓库ID',

        -- 车辆
        vehicleno varchar COMMENT '车辆ID',

        -- 状态
        is_print tinyint COMMENT '是否打印'
    ) UNIQUE KEY (dispatchid) DISTRIBUTED BY HASH (dispatchid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO dwd.logistics_tms_dispatch_doc (
    dispatchid,
    dw_updatetime,
    create_time,
    warehid,
    vehicleno,
    is_print
)
SELECT 
    dispatchid,
    dw_updatetime,
    credate AS create_time,
    warehid,
    vehicleno,
    IFNULL(printflag, 0) AS is_print
FROM
    ods_wms.TMS_luyan_DISPATCH_DOC
WHERE is_active = 1;