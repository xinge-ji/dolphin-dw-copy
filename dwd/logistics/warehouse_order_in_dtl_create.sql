DROP TABLE IF EXISTS dwd.logistics_warehouse_order_in_dtl;

CREATE TABLE
    dwd.logistics_warehouse_order_in_dtl (
        -- 主键标识
        indtlid bigint COMMENT '入库细单ID',
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 主单据
        inid bigint COMMENT '入库单ID',

        -- 仓库
        warehid bigint COMMENT '仓库ID',
        warehouse_name varchar COMMENT '仓库名称',

        -- 货主
        goodsownerid bigint COMMENT '货主ID',
        goodsowner_name varchar COMMENT '货主名称',

        -- 时间
        receive_time datetime COMMENT '收货时间',

        -- 商品
        goodsid bigint COMMENT '商品ID',
        goods_name varchar COMMENT '商品名称',
        is_coldchain tinyint COMMENT '是否冷链',
        is_chinese_medicine tinyint COMMENT '是否中药',

        -- 收货
        shrid bigint COMMENT '收货人ID',
        is_recheck tinyint COMMENT '是否验收差异单'
    ) UNIQUE KEY (indtlid, dw_updatetime) DISTRIBUTED BY HASH (indtlid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO 
    dwd.logistics_warehouse_order_in_dtl (
        indtlid,
        dw_updatetime,
        inid,
        warehid,
        warehouse_name,
        goodsownerid,
        goodsowner_name,
        receive_time,
        goodsid,
        goods_name,
        is_coldchain,
        is_chinese_medicine,
        shrid,
        is_recheck
    )
SELECT 
    b.indtlid,
    b.dw_updatetime AS dw_updatetime,
    b.inid,
    a.warehid,
    a.warehouse_name,
    a.goodsownerid,
    a.goodsowner_name,
    b.shdate AS receive_time,
    b.goodsid,
    g.goods_name,
    IFNULL(d.is_coldchain, 0),
    IFNULL(d.is_chinese_medicine, 0),
    b.shrid,
    COALESCE(b.recheckflag, 0) AS is_recheck
FROM 
    dwd.logistics_warehouse_order_in_doc a
JOIN 
    ods_wms.wms_in_order_dtl b ON a.inid = b.inid
LEFT JOIN 
    dim.goods g ON b.goodsid = g.goodsid AND a.create_time >= g.dw_starttime AND a.create_time < g.dw_endtime
JOIN 
    dim.wms_goods_feature d ON a.warehid = d.warehid AND b.goodsid = d.goodsid AND a.create_time >= d.dw_starttime AND a.create_time < d.dw_endtime
WHERE 
    b.is_active=1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_indtlid ON dwd.logistics_warehouse_order_in_dtl (indtlid);
CREATE INDEX IF NOT EXISTS idx_inid ON dwd.logistics_warehouse_order_in_dtl (inid);
CREATE INDEX IF NOT EXISTS idx_warehid ON dwd.logistics_warehouse_order_in_dtl (warehid);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dwd.logistics_warehouse_order_in_dtl (goodsid);
CREATE INDEX IF NOT EXISTS idx_receive_time ON dwd.logistics_warehouse_order_in_dtl (receive_time);
