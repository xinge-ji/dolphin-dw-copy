INSERT INTO dwd.logistics_warehouse_order_receive_dtl (receiveid, __DORIS_DELETE_SIGN__)
SELECT a.receiveid, 1
FROM ods_wms.wms_in_order_dtl AS a
JOIN dwd.logistics_warehouse_order_receive_dtl AS b ON a.receiveid = b.receiveid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;


INSERT INTO 
    dwd.logistics_warehouse_order_receive_dtl (
        receiveid,
        dw_updatetime,
        inid,
        indtlid,
        warehid,
        warehouse_name,
        sectionid,
        check_time,
        goodsid,
        goods_name,
        scatter_qty,
        whole_qty
    )
SELECT 
    r.receiveid,
    r.dw_updatetime,
    a.inid,
    r.indtlid,
    r.warehid,
    b.warehname AS warehouse_name,
    r.sectionid,
    r.checkdate AS check_time,
    r.goodsid,
    g.goods_name,
    r.scatterqty,
    r.wholeqty
FROM 
    ods_wms.wms_receive_dtl r
JOIN 
    dwd.logistics_warehouse_order_in_dtl a ON r.indtlid = a.indtlid
LEFT JOIN ods_wms.tpl_warehouse b ON r.warehid = b.warehid AND b.is_active = 1
LEFT JOIN 
    dim.goods g ON r.goodsid = g.goodsid AND r.checkdate >= g.dw_starttime AND r.checkdate < g.dw_endtime
WHERE r.is_active = 1
AND r.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_order_receive_dtl);
