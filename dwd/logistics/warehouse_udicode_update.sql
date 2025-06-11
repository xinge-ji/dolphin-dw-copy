INSERT INTO dwd.logistics_warehouse_udicode (recordid, __DORIS_DELETE_SIGN__)
SELECT a.recordid, 1
FROM ods_wms.wms_udicode_record AS a
JOIN dwd.logistics_warehouse_udicode AS b ON a.recordid = b.recordid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO 
    dwd.logistics_warehouse_udicode (
        recordid,
        dw_updatetime,
        create_time,
        warehid,
        warehouse_name,
        goodsownerid,
        goodsowner_name,
        goodsid,
        goods_name,
        is_coldchain,
        is_chinese_medicine,
        sourceid,
        is_out,
        udicode
    )
SELECT 
    e.recordid,
    e.dw_updatetime AS dw_updatetime,
    e.credate AS create_time,
    e.warehid,
    b.warehname,
    e.goodsownerid,
    c.goodsownername,
    e.goodsid,
    g.goods_name,
    IFNULL(d.is_coldchain, 0),
    IFNULL(d.is_chinese_medicine, 0),
    e.sourceid,
    IFNULL(e.INOUTFLAG, 0) as is_out,
    e.udicode
FROM 
    ods_wms.wms_udi_code_record e
LEFT JOIN ods_wms.tpl_warehouse b ON e.warehid = b.warehid AND b.is_active = 1
LEFT JOIN ods_wms.tpl_goodsowner c ON e.goodsownerid = c.goodsownerid AND c.is_active = 1
LEFT JOIN dim.goods g ON e.goodsid = g.goodsid AND e.credate >= g.dw_starttime AND e.credate < g.dw_endtime
LEFT JOIN dim.wms_goods_feature d ON e.warehid = d.warehid AND e.goodsid = d.goodsid AND e.credate >= d.dw_starttime AND e.credate < d.dw_endtime
WHERE 
    e.is_active = 1
    AND e.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_udicode);