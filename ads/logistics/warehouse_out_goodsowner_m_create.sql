DROP TABLE IF EXISTS ads.logistics_warehouse_out_goodsowner_m;
CREATE TABLE ads.logistics_warehouse_out_goodsowner_m(
    -- 颗粒度
    stat_yearmonth date COMMENT '统计年月',
    warehid bigint COMMENT '仓库ID',
    goodsownerid bigint COMMENT '货主ID',
    category varchar COMMENT '类别:总数/冷链/中药/其他',

    -- 描述
    warehouse_name varchar COMMENT '仓库名称',
    goodsowner_name varchar COMMENT '货主名称',

    -- 指标
    flat_pick_count int COMMENT '平库拣货条目数',
    flat_pick_whole_qty int COMMENT '平库拣货整件件数',
    flat_pick_scatter_count int COMMENT '平库拣货散件条目数',
    auto_pick_count int COMMENT '立库拣货条目数',
    auto_pick_whole_qty int COMMENT '立库拣货整件件数',
    auto_pick_scatter_count int COMMENT '立库拣货散件条目数',
    auto_udicode_count int COMMENT '立库UDI码',
    out_scatter_box_count int COMMENT '散件出库箱数',
    out_whole_qty int COMMENT '整件出库件数'
)
UNIQUE KEY(stat_yearmonth, warehid, goodsownerid, category) 
DISTRIBUTED BY HASH(stat_yearmonth, warehid, goodsownerid, category) 
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
);

INSERT INTO ads.logistics_warehouse_out_goodsowner_m (
    stat_yearmonth,
    warehid,
    goodsownerid,
    category,
    warehouse_name,
    goodsowner_name,
    flat_pick_count,
    flat_pick_whole_qty,
    flat_pick_scatter_count,
    auto_pick_count,
    auto_pick_whole_qty,
    auto_pick_scatter_count,
    auto_udicode_count,
    out_scatter_box_count,
    out_whole_qty
)
SELECT
    -- 颗粒度
    DATE_TRUNC(stat_date,'MONTH') AS stat_yearmonth,
    warehid,
    goodsownerid,
    category,
    
    -- 维度
    MAX(warehouse_name) AS warehouse_name,
    MAX(goodsowner_name) AS goodsowner_name,
    
    -- 指标聚合
    SUM(flat_pick_count) AS flat_pick_count,
    SUM(flat_pick_whole_qty) AS flat_pick_whole_qty,
    SUM(flat_pick_scatter_count) AS flat_pick_scatter_count,
    SUM(auto_pick_count) AS auto_pick_count,
    SUM(auto_pick_whole_qty) AS auto_pick_whole_qty,
    SUM(auto_pick_scatter_count) AS auto_pick_scatter_count,
    SUM(auto_udicode_count) AS auto_udicode_count,
    SUM(out_scatter_box_count) AS out_scatter_box_count,
    SUM(out_whole_qty) AS out_whole_qty
FROM 
    dws.logistics_warehouse_out_goodsowner_d
WHERE 
    stat_date < DATE_TRUNC(CURRENT_DATE(), 'MONTH')
GROUP BY 
    DATE_TRUNC(stat_date,'MONTH'),
    warehid,
    goodsownerid,
    category;