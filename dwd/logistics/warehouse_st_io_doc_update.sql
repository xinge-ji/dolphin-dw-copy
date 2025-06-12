INSERT INTO dwd.logistics_warehouse_st_io_doc (inoutid, __DORIS_DELETE_SIGN__)
SELECT a.inoutid, 1
FROM ods_wms.wms_st_io_doc AS a
JOIN dwd.logistics_warehouse_st_io_doc AS b ON a.inoutid = b.inoutid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO 
    dwd.logistics_warehouse_st_io_doc (
        inoutid,
        dw_updatetime,
        create_time,
        finish_time,
        is_out,
        comefrom,
        rfflag,
        sourceid,
        goodsownerid,
        goodsowner_name,
        warehid,
        warehouse_name,
        rfmanid,
        sectionid,
        goodsid,
        goods_name,
        is_coldchain,
        is_chinese_medicine,
        whole_qty,
        scatter_qty
    )
SELECT 
    io.inoutid,
    io.dw_updatetime AS dw_updatetime,
    io.credate AS create_time,
    io.rffindate AS finish_time,
    io.inoutflag AS is_out,
    io.comefrom,
    io.rfflag,
    io.sourceid,
    io.goodsownerid,
    c.goodsownername,
    io.warehid,
    w.warehname AS warehouse_name,
    io.rfmanid,
    io.sectionid,
    io.goodsid,
    g.goods_name,
    d.is_coldchain,
    d.is_chinese_medicine,
    io.wholeqty AS whole_qty,
    io.scatterqty AS scatter_qty
FROM 
    ods_wms.wms_st_io_doc io
LEFT JOIN 
    ods_wms.tpl_warehouse w ON io.warehid = w.warehid AND w.is_active = 1
LEFT JOIN 
    dim.goods g ON io.goodsid = g.goodsid AND io.credate >= g.dw_starttime AND io.credate < g.dw_endtime
JOIN 
    dim.wms_goods_feature d ON io.warehid = d.warehid AND io.goodsid = d.goodsid AND io.credate >= d.dw_starttime AND io.credate < d.dw_endtime
LEFT JOIN ods_wms.tpl_goodsowner c ON io.goodsownerid = c.goodsownerid AND c.is_active = 1
WHERE 
    io.is_active = 1
    AND io.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_st_io_doc);
