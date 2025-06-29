DROP TABLE IF EXISTS dwd.logistics_warehouse_order_receive_dtl;

CREATE TABLE
    dwd.logistics_warehouse_order_receive_dtl (
        -- 主键标识
        receiveid bigint COMMENT '验收细单ID',
        dw_updatetime datetime COMMENT '数据更新时间',

        -- 关联单据
        indtlid bigint COMMENT '入库细单ID',

        -- 仓库
        warehid bigint COMMENT '仓库ID',
        warehouse_name varchar COMMENT '仓库名称',
        sectionid bigint COMMENT '区域ID',
        section_name varchar COMMENT '分区名称',

        -- 货主
        goodsownerid bigint COMMENT '货主ID',
        goodsowner_name varchar COMMENT '货主名称',

        -- 时间
        check_time datetime COMMENT '验收时间',

        -- 商品
        goodsid bigint COMMENT '商品ID',
        goods_name varchar COMMENT '商品名称',
        goods_category varchar COMMENT '商品分类:冷链/中药/其他',
        scatter_qty decimal(16, 6) COMMENT '散件数量',
        whole_qty decimal COMMENT '整件数量'
    ) UNIQUE KEY (receiveid) DISTRIBUTED BY HASH (receiveid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO 
    dwd.logistics_warehouse_order_receive_dtl (
        receiveid,
        dw_updatetime,
        indtlid,
        warehid,
        warehouse_name,
        sectionid,
        section_name,
        goodsownerid,
        goodsowner_name,
        check_time,
        goodsid,
        goods_name,
        goods_category,
        scatter_qty,
        whole_qty
    )
-- 优化后的查询
WITH filtered_receive AS (
    SELECT receiveid, dw_updatetime, indtlid, warehid, sectionid, checkdate, goodsid, scatterqty, wholeqty
    FROM ods_wms.wms_receive_dtl 
    WHERE is_active = 1 
),
latest_shelf AS (
    -- 只处理有关联的shelf数据
    SELECT 
        sd.sourceid AS receiveid, 
        sd.section_name,
        ROW_NUMBER() OVER (PARTITION BY sd.sourceid ORDER BY sd.shelf_time ASC, sd.section_name ASC) as rn
    FROM dwd.logistics_warehouse_shelf_doc sd
    WHERE sd.section_name IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM filtered_receive fr WHERE fr.receiveid = sd.sourceid
      )
)
SELECT 
    r.receiveid,
    r.dw_updatetime,
    r.indtlid,
    r.warehid,
    d.warehouse_name,
    r.sectionid,
    COALESCE(j.sectionname, ls.section_name, '其他') AS section_name,
    d.goodsownerid,
    d.goodsowner_name,
    r.checkdate AS check_time,
    r.goodsid,
    d.goods_name,
    IFNULL(d.goods_category, '其他') AS goods_category,
    r.scatterqty,
    r.wholeqty
FROM filtered_receive r
JOIN dwd.logistics_warehouse_order_in_dtl d ON r.indtlid = d.indtlid
LEFT JOIN (SELECT sectionid, sectionname FROM ods_wms.wms_st_section_def WHERE is_active = 1) j ON r.sectionid = j.sectionid
LEFT JOIN (SELECT receiveid, section_name FROM latest_shelf WHERE rn = 1) ls ON r.receiveid = ls.receiveid;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_receiveid ON dwd.logistics_warehouse_order_receive_dtl (receiveid);
CREATE INDEX IF NOT EXISTS idx_warehid ON dwd.logistics_warehouse_order_receive_dtl (warehid);
CREATE INDEX IF NOT EXISTS idx_goodsownerid ON dwd.logistics_warehouse_order_receive_dtl (goodsownerid);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dwd.logistics_warehouse_order_receive_dtl (goodsid);
