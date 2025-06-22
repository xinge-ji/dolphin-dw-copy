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
        create_time,
        create_date,
        receive_time,
        receive_date,
        order_to_receive_time,
        goodsid,
        goods_name,
        goods_category,
        shrid,
        shr_name,
        operation_type,
        is_recheck,
        is_autotask
    )
SELECT 
    b.indtlid,
    b.dw_updatetime,
    b.inid,
    a.warehid,
    a.warehouse_name,
    a.goodsownerid,
    a.goodsowner_name,
    a.create_time,
    DATE(a.create_time) AS create_date,
    b.shdate AS receive_time,
    DATE(b.shdate) AS receive_date,
    DATEDIFF(DATE(b.shdate), DATE(a.create_time)) AS order_to_receive_time,
    b.goodsid,
    g.goods_name,
    IFNULL(d.goods_category, '其他') AS goods_category,
    b.shrid,
    r.employeename AS shr_name,
    a.operation_type,
    COALESCE(b.recheckflag, 0) AS is_recheck,
    COALESCE(a.is_autotask, 0) AS is_autotask
FROM 
    dwd.logistics_warehouse_order_in_doc a
JOIN 
    ods_wms.wms_in_order_dtl b ON a.inid = b.inid
LEFT JOIN 
    dim.goods g ON b.goodsid = g.goodsid AND a.create_time >= g.dw_starttime AND a.create_time < g.dw_endtime
LEFT JOIN 
    dim.wms_goods_feature d ON a.warehid = d.warehid AND b.goodsid = d.goodsid AND a.create_time >= d.dw_starttime AND a.create_time < d.dw_endtime
LEFT JOIN 
    ods_wms.pub_employee r ON b.shrid = r.employeeid AND r.is_active = 1
WHERE 
    b.is_active = 1
    AND b.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_order_in_dtl);