DROP TABLE IF EXISTS dwd.logistics_warehouse_st_io_doc;

CREATE TABLE
    dwd.logistics_warehouse_st_io_doc (
        -- 主键标识
        inoutid bigint COMMENT '出入库单ID',
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 时间
        create_time datetime COMMENT '建立时间',
        finish_time datetime COMMENT '完成时间', 

        -- 关联单据
        sourceid bigint COMMENT '来源单据ID',

        -- 货主
        goodsownerid bigint COMMENT '货主ID',
        goodsowner_name varchar COMMENT '货主名称',

        -- 状态
        is_out int COMMENT '是否出库单',
        comefrom int COMMENT '来源',
        rfflag tinyint COMMENT '状态:2-待完成;3-已完成',

        -- 仓库
        warehid bigint COMMENT '仓库ID',
        warehouse_name varchar COMMENT '仓库名称',
        rfmanid bigint COMMENT '单据处理人ID',
        sectionid bigint COMMENT '区域ID',

        -- 商品
        goodsid bigint COMMENT '商品ID',
        goods_name varchar COMMENT '商品名称',
        is_coldchain tinyint COMMENT '是否冷链',
        is_chinese_medicine tinyint COMMENT '是否中药',
        whole_qty decimal COMMENT '整件件数',
        scatter_qty decimal(16,6) COMMENT '散件件数'        
    ) UNIQUE KEY (inoutid, dw_updatetime) DISTRIBUTED BY HASH (inoutid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO 
    dwd.logistics_warehouse_st_io_doc (
        inoutid,
        dw_updatetime,
        create_time,
        finish_time,
        is_out,
        comefrom,
        rfflag,
        sourceid,
        goodsownerid,
        goodsowner_name,
        warehid,
        warehouse_name,
        rfmanid,
        sectionid,
        goodsid,
        goods_name,
        is_coldchain,
        is_chinese_medicine,
        whole_qty,
        scatter_qty
    )
SELECT 
    io.inoutid,
    io.dw_updatetime AS dw_updatetime,
    io.credate AS create_time,
    io.rffindate AS finish_time,
    io.inoutflag AS is_out,
    io.comefrom,
    io.rfflag,
    io.sourceid,
    io.goodsownerid,
    c.goodsownername,
    io.warehid,
    w.warehname AS warehouse_name,
    io.rfmanid,
    io.sectionid,
    io.goodsid,
    g.goods_name,
    d.is_coldchain,
    d.is_chinese_medicine,
    io.wholeqty AS whole_qty,
    io.scatterqty AS scatter_qty
FROM 
    ods_wms.wms_st_io_doc io
LEFT JOIN 
    ods_wms.tpl_warehouse w ON io.warehid = w.warehid AND w.is_active = 1
LEFT JOIN 
    dim.goods g ON io.goodsid = g.goodsid AND io.credate >= g.dw_starttime AND io.credate < g.dw_endtime
JOIN 
    dim.wms_goods_feature d ON io.warehid = d.warehid AND io.goodsid = d.goodsid AND io.credate >= d.dw_starttime AND io.credate < d.dw_endtime
LEFT JOIN ods_wms.tpl_goodsowner c ON io.goodsownerid = c.goodsownerid AND c.is_active = 1
WHERE 
    io.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_inoutid ON dwd.logistics_warehouse_st_io_doc (inoutid);
CREATE INDEX IF NOT EXISTS idx_warehid ON dwd.logistics_warehouse_st_io_doc (warehid);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dwd.logistics_warehouse_st_io_doc (goodsid);
CREATE INDEX IF NOT EXISTS idx_create_time ON dwd.logistics_warehouse_st_io_doc (create_time);
CREATE INDEX IF NOT EXISTS idx_finish_time ON dwd.logistics_warehouse_st_io_doc (finish_time);