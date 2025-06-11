INSERT INTO dwd.logistics_warehouse_order_in_dtl (indtlid, __DORIS_DELETE_SIGN__)
SELECT a.indtlid, 1
FROM ods_wms.wms_in_order_dtl AS a
JOIN dwd.logistics_warehouse_order_in_dtl AS b ON a.indtlid = b.indtlid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO 
    dwd.logistics_warehouse_order_in_dtl (
        indtlid,
        dw_updatetime,
        inid,
        warehid,
        warehouse_name,
        goodsownerid,
        goodsowner_name,
        receive_time,
        goodsid,
        goods_name,
        is_coldchain,
        is_chinese_medicine,
        shrid,
        is_recheck
    )
SELECT 
    b.indtlid,
    b.dw_updatetime AS dw_updatetime,
    b.inid,
    a.warehid,
    a.warehouse_name,
    a.goodsownerid,
    a.goodsowner_name,
    b.shdate AS receive_time,
    b.goodsid,
    g.goods_name,
    d.is_coldchain,
    d.is_chinese_medicine,
    b.shrid,
    COALESCE(b.recheckflag, 0) AS is_recheck
FROM 
    dwd.logistics_warehouse_order_in_doc a
JOIN 
    ods_wms.wms_in_order_dtl b ON a.inid = b.inid
LEFT JOIN 
    dim.goods g ON b.goodsid = g.goodsid AND a.create_time >= g.dw_starttime AND a.create_time < g.dw_endtime
JOIN 
    dim.wms_goods_feature d ON a.warehid = d.warehid AND b.goodsid = d.goodsid AND a.create_time >= d.dw_starttime AND a.create_time < d.dw_endtime
WHERE 
    b.is_active=1
    AND b.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_order_in_dtl);