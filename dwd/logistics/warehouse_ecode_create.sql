DROP TABLE IF EXISTS dwd.logistics_warehouse_ecode;

CREATE TABLE
    dwd.logistics_warehouse_ecode (
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
        goods_category varchar COMMENT '商品分类:冷链/中药/其他',

        -- 关联单据
        sourceid bigint COMMENT '来源单据ID',
        is_out tinyint COMMENT '是否出库',

        -- 监管码
        ecode bigint COMMENT '监管码'
    ) UNIQUE KEY (recordid, dw_updatetime) DISTRIBUTED BY HASH (recordid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO dwd.logistics_warehouse_ecode (
    recordid,
    dw_updatetime,
    create_time,
    warehid,
    warehouse_name,
    goodsownerid,
    goodsowner_name,
    goodsid,
    goods_name,
    goods_category,
    sourceid,
    is_out,
    ecode
)
SELECT 
    a.recordid,
    a.dw_updatetime,
    a.credate AS create_time,
    a.warehid,
    b.warehname AS warehouse_name,
    a.goodsownerid,
    c.goodsownername AS goodsowner_name,
    d.waregoodsid AS goodsid,
    d.goodsname AS goods_name,
    CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(IFNULL(a.srcexpno, CAST(a.sourceid AS STRING)), '\n', ''), '\r', ''), '\t', '') AS BIGINT) AS sourceid,
    IFNULL(e.goods_category, '其他') AS goods_category,
    a.inoutflag AS is_out, -- 1=出库, 0=入库
    a.ecode
FROM ods_wms.wms_ecode_record a
LEFT JOIN ods_wms.tpl_warehouse b ON a.warehid = b.warehid AND b.is_active = 1
LEFT JOIN ods_wms.tpl_goodsowner c ON a.goodsownerid = c.goodsownerid AND c.is_active = 1
LEFT JOIN ods_wms.tpl_goods d ON a.ownergoodsid = d.ownergoodsid AND d.is_active = 1
LEFT JOIN dim.wms_goods_feature e ON a.warehid = e.warehid AND d.waregoodsid = e.goodsid AND a.credate >= e.dw_starttime AND a.credate < e.dw_endtime
WHERE a.is_active = 1;