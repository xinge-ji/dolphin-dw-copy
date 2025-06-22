DROP TABLE IF EXISTS dwd.logistics_warehouse_order_in_dtl;

CREATE TABLE
    dwd.logistics_warehouse_order_in_dtl (
        -- 主键标识
        indtlid bigint COMMENT '入库细单ID',

        -- 数据更新时间
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 主单
        inid bigint COMMENT '入库单ID',

        -- 仓库
        warehid bigint COMMENT '仓库ID',
        warehouse_name varchar COMMENT '仓库名称',

        -- 货主
        goodsownerid bigint COMMENT '货主ID',
        goodsowner_name varchar COMMENT '货主名称',

        -- 时间
        create_time datetime COMMENT '建立时间',
        create_date date COMMENT '建立日期',
        receive_time datetime COMMENT '收货时间',
        receive_date date COMMENT '收货日期',
        order_to_receive_time bigint COMMENT '订单到收货时间(天)',

        -- 商品
        goodsid bigint COMMENT '商品ID',
        goods_name varchar COMMENT '商品名称',
        goods_category varchar COMMENT '商品分类:冷链/中药/其他',

        -- 收货
        shrid bigint COMMENT '收货人ID',
        shr_name varchar COMMENT '收货人名称',

        -- 操作
        operation_type varchar COMMENT '业务类型描述',
        is_recheck tinyint COMMENT '是否验收差异单',
        is_autotask tinyint COMMENT '是否转单自动生成订单'
    ) UNIQUE KEY (indtlid) DISTRIBUTED BY HASH (indtlid) PROPERTIES (
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
        create_time,
        create_date,
        receive_time,
        receive_date,
        order_to_receive_time,
        goodsid,
        goods_name,
        goods_category,
        shrid,
        shr_name,
        operation_type,
        is_recheck,
        is_autotask
    )
SELECT 
    b.indtlid,
    b.dw_updatetime,
    b.inid,
    a.warehid,
    a.warehouse_name,
    a.goodsownerid,
    a.goodsowner_name,
    a.create_time,
    DATE(a.create_time) AS create_date,
    b.shdate AS receive_time,
    DATE(b.shdate) AS receive_date,
    DATEDIFF(DATE(b.shdate), DATE(a.create_time)) AS order_to_receive_time,
    b.goodsid,
    g.goods_name,
    IFNULL(d.goods_category, '其他') AS goods_category,
    b.shrid,
    r.employeename AS shr_name,
    a.operation_type,
    COALESCE(b.recheckflag, 0) AS is_recheck,
    COALESCE(a.is_autotask, 0) AS is_autotask
FROM 
    dwd.logistics_warehouse_order_in_doc a
JOIN 
    ods_wms.wms_in_order_dtl b ON a.inid = b.inid
LEFT JOIN 
    dim.goods g ON b.goodsid = g.goodsid AND a.create_time >= g.dw_starttime AND a.create_time < g.dw_endtime
LEFT JOIN 
    dim.wms_goods_feature d ON a.warehid = d.warehid AND b.goodsid = d.goodsid AND a.create_time >= d.dw_starttime AND a.create_time < d.dw_endtime
LEFT JOIN 
    ods_wms.pub_employee r ON b.shrid = r.employeeid AND r.is_active = 1
WHERE 
    b.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_indtlid ON dwd.logistics_warehouse_order_in_dtl (indtlid);
CREATE INDEX IF NOT EXISTS idx_inid ON dwd.logistics_warehouse_order_in_dtl (inid);
CREATE INDEX IF NOT EXISTS idx_warehid ON dwd.logistics_warehouse_order_in_dtl (warehid);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dwd.logistics_warehouse_order_in_dtl (goodsid);
CREATE INDEX IF NOT EXISTS idx_receive_time ON dwd.logistics_warehouse_order_in_dtl (receive_time);
