DROP TABLE IF EXISTS dwd.logistics_warehouse_udicode;

CREATE TABLE
    dwd.logistics_warehouse_udicode (
        -- 主键标识
        recordid bigint COMMENT '单据ID',
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 时间
        create_time datetime COMMENT '建立时间',

        -- 仓库
        warehid bigint COMMENT '仓库ID',
        warehouse_name varchar COMMENT '仓库名称',

        -- 货主
        goodsownerid bigint COMMENT '货主ID',
        goodsowner_name varchar COMMENT '货主名称',

        -- 商品
        goodsid bigint COMMENT '商品ID',
        goods_name varchar COMMENT '商品名称',
        is_coldchain tinyint COMMENT '是否冷链',
        is_chinese_medicine tinyint COMMENT '是否中药',

        -- 关联单据
        sourceid bigint COMMENT '来源单据ID',
        is_out tinyint COMMENT '是否出库',

        -- udi码
        udicode bigint COMMENT 'udi码'
    ) UNIQUE KEY (recordid, dw_updatetime) DISTRIBUTED BY HASH (recordid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO 
    dwd.logistics_warehouse_udicode (
        recordid,
        dw_updatetime,
        create_time,
        warehid,
        warehouse_name,
        goodsownerid,
        goodsowner_name,
        goodsid,
        goods_name,
        is_coldchain,
        is_chinese_medicine,
        sourceid,
        is_out,
        udicode
    )
SELECT 
    e.recordid,
    e.dw_updatetime AS dw_updatetime,
    e.credate AS create_time,
    e.warehid,
    b.warehname,
    e.goodsownerid,
    c.goodsownername,
    e.goodsid,
    g.goods_name,
    IFNULL(d.is_coldchain, 0),
    IFNULL(d.is_chinese_medicine, 0),
    e.sourceid,
    IFNULL(e.INOUTFLAG, 0) as is_out,
    e.udicode
FROM 
    ods_wms.wms_udi_code_record e
LEFT JOIN ods_wms.tpl_warehouse b ON e.warehid = b.warehid AND b.is_active = 1
LEFT JOIN ods_wms.tpl_goodsowner c ON e.goodsownerid = c.goodsownerid AND c.is_active = 1
LEFT JOIN dim.goods g ON e.goodsid = g.goodsid AND e.credate >= g.dw_starttime AND e.credate < g.dw_endtime
LEFT JOIN dim.wms_goods_feature d ON e.warehid = d.warehid AND e.goodsid = d.goodsid AND e.credate >= d.dw_starttime AND e.credate < d.dw_endtime
WHERE 
    e.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_recordid ON dwd.logistics_warehouse_udicode (recordid);
CREATE INDEX IF NOT EXISTS idx_sourceid ON dwd.logistics_warehouse_udicode (sourceid);
CREATE INDEX IF NOT EXISTS idx_udicode ON dwd.logistics_warehouse_udicode (udicode);
CREATE INDEX IF NOT EXISTS idx_create_time ON dwd.logistics_warehouse_udicode (create_time);