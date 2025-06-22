INSERT INTO dwd.logistics_warehouse_order_out_lot_dtl (outlotid, __DORIS_DELETE_SIGN__)
SELECT a.outlotid, 1
FROM ods_wms.wms_out_order_lot_dtl AS a
JOIN dwd.logistics_warehouse_order_out_lot_dtl AS b ON a.outlotid = b.outlotid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.logistics_warehouse_order_out_lot_dtl (
    outlotid,
    dw_updatetime,
    outdtlid
)
SELECT
    outlotid,
    dw_updatetime,
    outdtlid
FROM ods_wms.wms_out_order_lot_dtl
WHERE is_active = 1
AND dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_order_out_lot_dtl);