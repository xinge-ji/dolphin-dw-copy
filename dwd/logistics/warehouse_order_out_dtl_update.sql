INSERT INTO dwd.logistics_warehouse_order_out_dtl (outdtlid, __DORIS_DELETE_SIGN__)
SELECT a.outdtlid, 1
FROM ods_wms.wms_out_order_dtl AS a
JOIN dwd.logistics_warehouse_order_out_dtl AS b ON a.outdtlid = b.outdtlid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.logistics_warehouse_order_out_dtl (
    outdtlid,
    dw_updatetime,
    outid,
    goodsownerid,
    warehid,
    goodsid,
    ownergoodsid
)
SELECT 
    outdtlid,
    dw_updatetime,
    outid,
    goodsownerid,
    warehid,
    goodsid,
    ownergoodsid
FROM ods_wms.wms_out_order_dtl
WHERE is_active = 1
AND dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_order_out_dtl);