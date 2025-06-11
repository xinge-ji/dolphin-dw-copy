DROP TABLE IF EXISTS ads.logistics_warehouse_in_goodsowner_m;
CREATE TABLE ads.logistics_warehouse_in_goodsowner_m(
    -- 颗粒度
    stat_yearmonth date COMMENT '统计年月',
    warehid bigint COMMENT '仓库ID',
    goodsownerid bigint COMMENT '货主ID',
    category varchar COMMENT '类别:总数/冷链/中药/其他',

    -- 描述
    warehouse_name varchar COMMENT '仓库名称',
    goodsowner_name varchar COMMENT '货主名称',

    -- 指标
    receive_count int COMMENT '收货条目数',
    check_count int COMMENT '验收条目数',
    check_scatter_count int COMMENT '散件验收条目数',
    check_whole_count int COMMENT '整件验收条目数',
    check_scatter_qty decimal(16,6) COMMENT '散件验收件数',
    check_whole_qty decimal COMMENT '整件验收件数',
    flat_shelf_whole_qty decimal COMMENT '平库上架整件件数',
    flat_shelf_scatter_count int COMMENT '平库上架散件条目数',
    auto_shelf_whole_qty decimal COMMENT '立库上架整件件数',
    auto_shelf_scatter_count int COMMENT '立库上架散件条目数',
    ecode_count int COMMENT '电子监管码',
    udicode_count int COMMENT 'UDI码'
)
UNIQUE KEY(stat_yearmonth, warehid, goodsownerid, category) 
DISTRIBUTED BY HASH(stat_yearmonth, warehid, goodsownerid, category) 
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
);

INSERT INTO ads.logistics_warehouse_in_goodsowner_m (
    -- 颗粒度
    stat_yearmonth,
    warehid,
    goodsownerid,
    category,

    -- 描述
    warehouse_name,
    goodsowner_name,
    
    -- 指标
    receive_count,
    check_count,
    check_scatter_count,
    check_whole_count,
    check_scatter_qty,
    check_whole_qty,
    flat_shelf_whole_qty,
    flat_shelf_scatter_count,
    auto_shelf_whole_qty,
    auto_shelf_scatter_count,
    ecode_count,
    udicode_count
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
    SUM(receive_count) AS receive_count,
    SUM(check_count) AS check_count,
    SUM(check_scatter_count) AS check_scatter_count,
    SUM(check_whole_count) AS check_whole_count,
    SUM(check_scatter_qty) AS check_scatter_qty,
    SUM(check_whole_qty) AS check_whole_qty,
    SUM(flat_shelf_whole_qty) AS flat_shelf_whole_qty,
    SUM(flat_shelf_scatter_count) AS flat_shelf_scatter_count,
    SUM(auto_shelf_whole_qty) AS auto_shelf_whole_qty,
    SUM(auto_shelf_scatter_count) AS auto_shelf_scatter_count,
    SUM(ecode_count) AS ecode_count,
    SUM(udicode_count) AS udicode_count
FROM 
    dws.logistics_warehouse_in_goodsowner_d
WHERE 
    stat_date < DATE_TRUNC(CURRENT_DATE(), 'MONTH')
GROUP BY 
    DATE_TRUNC(stat_date,'MONTH'),
    warehid,
    goodsownerid,
    category;