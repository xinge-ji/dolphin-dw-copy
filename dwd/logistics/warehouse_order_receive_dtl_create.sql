DROP TABLE IF EXISTS dwd.logistics_warehouse_order_receive_dtl;

CREATE TABLE
    dwd.logistics_warehouse_order_receive_dtl (
        -- 主键标识
        receiveid bigint COMMENT '验收细单ID',
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 关联单据
        inid bigint COMMENT '入库单ID',
        indtlid bigint COMMENT '入库细单ID',

        -- 仓库
        warehid bigint COMMENT '仓库ID',
        warehouse_name varchar COMMENT '仓库名称',
        sectionid bigint COMMENT '区域ID',

        -- 时间
        check_time datetime COMMENT '验收时间',

        -- 商品
        goodsid bigint COMMENT '商品ID',
        goods_name varchar COMMENT '商品名称',
        scatter_qty decimal(16, 6) COMMENT '散件数量',
        whole_qty decimal COMMENT '整件数量'
    ) UNIQUE KEY (receiveid, dw_updatetime) DISTRIBUTED BY HASH (receiveid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );


INSERT INTO 
    dwd.logistics_warehouse_order_receive_dtl (
        receiveid,
        dw_updatetime,
        inid,
        indtlid,
        warehid,
        warehouse_name,
        sectionid,
        check_time,
        goodsid,
        goods_name,
        scatter_qty,
        whole_qty
    )
SELECT 
    r.receiveid,
    r.dw_updatetime,
    a.inid,
    r.indtlid,
    r.warehid,
    b.warehname AS warehouse_name,
    r.sectionid,
    r.checkdate AS check_time,
    r.goodsid,
    g.goods_name,
    r.scatterqty,
    r.wholeqty
FROM 
    ods_wms.wms_receive_dtl r
JOIN 
    dwd.logistics_warehouse_order_in_dtl a ON r.indtlid = a.indtlid
LEFT JOIN ods_wms.tpl_warehouse b ON r.warehid = b.warehid AND b.is_active = 1
LEFT JOIN 
    dim.goods g ON r.goodsid = g.goodsid AND r.checkdate >= g.dw_starttime AND r.checkdate < g.dw_endtime
WHERE r.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_receiveid ON dwd.logistics_warehouse_order_receive_dtl (receiveid);
CREATE INDEX IF NOT EXISTS idx_inid ON dwd.logistics_warehouse_order_receive_dtl (inid);
CREATE INDEX IF NOT EXISTS idx_warehid ON dwd.logistics_warehouse_order_receive_dtl (warehid);
CREATE INDEX IF NOT EXISTS idx_goodsownerid ON dwd.logistics_warehouse_order_receive_dtl (goodsownerid);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dwd.logistics_warehouse_order_receive_dtl (goodsid);
