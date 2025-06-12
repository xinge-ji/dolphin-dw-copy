INSERT INTO
    ads.logistics_warehouse_in_goodsowner_m (
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
    DATE_TRUNC (stat_date, 'MONTH') AS stat_yearmonth,
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
    DATE_FORMAT(stat_date, '%Y%m') IN (
        DATE_FORMAT(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 1 MONTH), '%Y%m'),
        DATE_FORMAT(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), 'MONTH'), INTERVAL 2 MONTH), '%Y%m')
    )
GROUP BY
    DATE_TRUNC (stat_date, 'MONTH'),
    warehid,
    goodsownerid,
    category;