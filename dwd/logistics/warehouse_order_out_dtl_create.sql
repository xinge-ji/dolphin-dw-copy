DROP TABLE IF EXISTS dwd.logistics_warehouse_order_out_dtl;

CREATE TABLE
    dwd.logistics_warehouse_order_out_dtl (
        -- 主键标识
        outdtlid bigint COMMENT '出库订单细单ID',

        -- 数据更新时间
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 关联单据
        outid bigint COMMENT '出库订单总单ID',
        goodsownerid bigint COMMENT '货主ID',
        warehid bigint COMMENT '仓库ID',
        
        -- 货品
        goodsid bigint COMMENT '商品ID',
        ownergoodsid bigint COMMENT '货主商品ID'
    ) UNIQUE KEY (outdtlid) DISTRIBUTED BY HASH (outdtlid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO dwd.logistics_warehouse_order_out_dtl (
    outdtlid,
    dw_updatetime,
    outid,
    goodsownerid,
    warehid,
    goodsid,
    ownergoodsid
)
SELECT 
    outdtlid,
    dw_updatetime,
    outid,
    goodsownerid,
    warehid,
    goodsid,
    ownergoodsid
FROM ods_wms.wms_out_order_dtl
WHERE is_active = 1;