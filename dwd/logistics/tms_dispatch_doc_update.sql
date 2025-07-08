INSERT INTO dwd.logistics_tms_dispatch_doc (dispatchid, __DORIS_DELETE_SIGN__)
SELECT a.dispatchid, 1
FROM ods_wms.TMS_luyan_DISPATCH_DOC AS a
JOIN dwd.logistics_tms_dispatch_doc AS b ON a.dispatchid = b.dispatchid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.logistics_tms_dispatch_doc (
    dispatchid,
    dw_updatetime,
    create_time,
    warehid,
    vehicleno,
    is_print
)
SELECT 
    dispatchid,
    dw_updatetime,
    credate AS create_time,
    warehid,
    vehicleno,
    IFNULL(printflag, 0) AS is_print
FROM
    ods_wms.TMS_luyan_DISPATCH_DOC
WHERE is_active = 1 AND dw_updatetime >= (SELECT MAX(dw_updatetime) FROM dwd.logistics_tms_dispatch_doc);