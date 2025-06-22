DROP TABLE IF EXISTS dwd.logistics_warehouse_order_out_lot_dtl;

CREATE TABLE
    dwd.logistics_warehouse_order_out_lot_dtl (
        -- 主键标识
        outlotid bigint COMMENT '出库订单批次明细ID',

        -- 数据更新时间
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 关联单据
        outdtlid bigint COMMENT '出库订单总单ID'
    ) UNIQUE KEY (outlotid) DISTRIBUTED BY HASH (outlotid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO dwd.logistics_warehouse_order_out_lot_dtl (
    outlotid,
    dw_updatetime,
    outdtlid
)
SELECT
    outlotid,
    dw_updatetime,
    outdtlid
FROM ods_wms.wms_out_order_lot_dtl
WHERE is_active = 1;