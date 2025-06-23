INSERT INTO dwd.logistics_warehouse_ecode (recordid, __DORIS_DELETE_SIGN__)
SELECT a.recordid, 1
FROM ods_wms.wms_ecode_record AS a
JOIN dwd.logistics_warehouse_ecode AS b ON a.recordid = b.recordid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;


INSERT INTO dwd.logistics_warehouse_ecode (
    recordid,
    dw_updatetime,
    create_time,
    warehid,
    warehouse_name,
    goodsownerid,
    goodsowner_name,
    goodsid,
    goods_name,
    goods_category,
    sourceid,
    is_out,
    ecode
)
SELECT 
    a.recordid,
    a.dw_updatetime,
    a.credate AS create_time,
    a.warehid,
    b.warehname AS warehouse_name,
    a.goodsownerid,
    c.goodsownername AS goodsowner_name,
    d.waregoodsid AS goodsid,
    d.goodsname AS goods_name,
    IFNULL(e.goods_category, '其他') AS goods_category,
    CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(IFNULL(a.srcexpno, CAST(a.sourceid AS STRING)), '\n', ''), '\r', ''), '\t', '') AS BIGINT) AS sourceid,
    a.inoutflag AS is_out, -- 1=出库, 0=入库
    a.ecode
FROM ods_wms.wms_ecode_record a
LEFT JOIN ods_wms.tpl_warehouse b ON a.warehid = b.warehid AND b.is_active = 1
LEFT JOIN ods_wms.tpl_goodsowner c ON a.goodsownerid = c.goodsownerid AND c.is_active = 1
LEFT JOIN ods_wms.tpl_goods d ON a.ownergoodsid = d.ownergoodsid AND d.is_active = 1
LEFT JOIN dim.wms_goods_feature e ON a.warehid = e.warehid AND d.waregoodsid = e.goodsid AND a.credate >= e.dw_starttime AND a.credate < e.dw_endtime
WHERE a.is_active = 1
    AND a.dw_updatetime > (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_ecode);