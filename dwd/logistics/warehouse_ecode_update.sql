INSERT INTO dwd.logistics_warehouse_ecode (recordid, __DORIS_DELETE_SIGN__)
SELECT a.recordid, 1
FROM ods_wms.wms_ecode_record AS a
JOIN dwd.logistics_warehouse_ecode AS b ON a.recordid = b.recordid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO 
    dwd.logistics_warehouse_ecode (
        recordid,
        dw_updatetime,
        create_time,
        sourceid,
        ecode
    )
SELECT 
    e.recordid,
    e.dw_updatetime AS dw_updatetime,
    e.credate AS create_time,
    e.sourceid,
    e.ecode
FROM 
    ods_wms.wms_ecode_record e
WHERE 
    e.is_active = 1
    AND e.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_ecode);