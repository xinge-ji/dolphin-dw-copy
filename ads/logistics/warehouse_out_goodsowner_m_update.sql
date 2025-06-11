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
    WHERE DATE_FORMAT(stat_date, '%Y%m') IN (
        DATE_FORMAT(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 1 MONTH), '%Y%m')
        DATE_FORMAT(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 2 MONTH), '%Y%m')
    )
GROUP BY 
    DATE_TRUNC(stat_date,'MONTH'),
    warehid,
    goodsownerid,
    category;