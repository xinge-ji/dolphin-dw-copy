INSERT INTO dwd.logistics_warehouse_wave_doc (wavedocid, __DORIS_DELETE_SIGN__)
SELECT a.wavedocid, 1
FROM ods_wms.wms_udicode_record AS a
JOIN dwd.logistics_warehouse_wave_doc AS b ON a.wavedocid = b.wavedocid
WHERE a.is_active = 0 AND a.dw_updatetime > b.dw_updatetime;

INSERT INTO 
    dwd.logistics_warehouse_wave_doc (
        wavedocid,
        dw_updatetime,
        create_time,
        execute_time,
        warehid,
        warehouse_name
    )
SELECT 
    w.wavedocid,
    w.dw_updatetime,
    w.credate,
    w.executetime,
    w.warehid,
    b.warehname
FROM 
    ods_wms.wms_wave_doc w
LEFT JOIN ods_wms.tpl_warehouse b ON w.warehid = b.warehid AND b.is_active = 1
WHERE 
    w.is_active = 1
    AND w.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_wave_doc);