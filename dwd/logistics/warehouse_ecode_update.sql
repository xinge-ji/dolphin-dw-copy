INSERT INTO dwd.logistics_warehouse_ecode (recordid, __DORIS_DELETE_SIGN__)
SELECT a.recordid, 1
FROM ods_wms.wms_ecode_record AS a
JOIN dwd.logistics_warehouse_ecode AS b ON a.recordid = b.recordid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;


INSERT INTO dwd.logistics_warehouse_ecode (
    recordid,
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
    ecode
)
WITH ecode_data AS (
    -- 出库电子监管码
    SELECT 
        a.recordid,
        a.dw_updatetime AS dw_updatetime,
        a.credate AS create_time,
        b.warehid,
        d.warehname AS warehouse_name,
        b.goodsownerid,
        j.goodsownername AS goodsowner_name,
        b.goodsid,
        e.goods_name,
        IFNULL(f.is_coldchain, 0) as is_coldchain,
        IFNULL(f.is_chinese_medicine, 0) as is_chinese_medicine,
        CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(o.srcexpno, '\n', ''), '\r', ''), '\t', '') AS BIGINT) AS sourceid,
        1 AS is_out, -- 出库
        a.ecode
    FROM ods_wms.wms_ecode_record a
    JOIN ods_wms.wms_st_io_doc b ON a.sourceid = b.inoutid
    LEFT JOIN ods_wms.wms_out_order_lot_dtl m ON b.sourceid = m.wavegoodsdtlid AND m.is_active = 1
    LEFT JOIN ods_wms.wms_out_order_dtl n ON n.outdtlid = m.outdtlid AND n.is_active = 1
    LEFT JOIN ods_wms.wms_out_order o ON o.outid = n.outid AND o.is_active = 1
    LEFT JOIN ods_wms.tpl_warehouse d ON b.warehid = d.warehid AND d.is_active = 1
    LEFT JOIN ods_wms.tpl_goodsowner j ON b.goodsownerid = j.goodsownerid AND j.is_active = 1
    LEFT JOIN dim.goods e ON b.goodsid = e.goodsid AND a.credate >= e.dw_starttime AND a.credate < e.dw_endtime
    LEFT JOIN dim.wms_goods_feature f ON b.warehid = f.warehid AND b.goodsid = f.goodsid AND a.credate >= f.dw_starttime AND a.credate < f.dw_endtime
    WHERE a.inoutflag = 1
        AND b.comefrom = 3
        AND IFNULL(a.comefrom, 0) = 0
        AND a.is_active = 1
        AND b.is_active = 1
        AND a.dw_updatetime > (SELECT MAX(dw_updatetime) FROM dwd.logistics_warehouse_ecode)

    UNION ALL

    -- 入库电子监管码
    SELECT DISTINCT
        a.recordid,
        a.dw_updatetime AS dw_updatetime,
        a.credate AS create_time,
        b.warehid,
        d.warehname AS warehouse_name,
        b.goodsownerid,
        j.goodsownername AS goodsowner_name,
        b.goodsid,
        e.goods_name,
        IFNULL(f.is_coldchain, 0) as is_coldchain,
        IFNULL(f.is_chinese_medicine, 0) as is_chinese_medicine,
        CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(b.sourceid, '\n', ''), '\r', ''), '\t', '') AS BIGINT) AS sourceid,
        0 AS is_out, -- 入库
        a.ecode
    FROM ods_wms.wms_ecode_record a
    JOIN ods_wms.wms_st_io_doc b ON a.sourceid = b.sourceid
    LEFT JOIN ods_wms.tpl_warehouse d ON b.warehid = d.warehid AND d.is_active = 1
    LEFT JOIN ods_wms.tpl_goodsowner j ON b.goodsownerid = j.goodsownerid AND j.is_active = 1
    LEFT JOIN dim.goods e ON b.goodsid = e.goodsid AND a.credate >= e.dw_starttime AND a.credate < e.dw_endtime
    LEFT JOIN dim.wms_goods_feature f ON b.warehid = f.warehid AND b.goodsid = f.goodsid AND a.credate >= f.dw_starttime AND a.credate < f.dw_endtime
    WHERE a.inoutflag = 0 -- 入库采集
        AND b.comefrom = 1
        AND IFNULL(a.comefrom, 0) = 0
        AND a.is_active = 1
        AND b.is_active = 1
        AND a.dw_updatetime > (SELECT MAX(dw_updatetime) FROM dwd.logistics_warehouse_ecode)

    UNION ALL

    -- 入库收货采集电子监管码
    SELECT 
        a.recordid,
        a.dw_updatetime AS dw_updatetime,
        a.credate AS create_time,
        b.warehid,
        d.warehname AS warehouse_name,
        b.goodsownerid,
        j.goodsownername AS goodsowner_name,
        c.goodsid,
        e.goods_name,
        IFNULL(f.is_coldchain, 0) as is_coldchain,
        IFNULL(f.is_chinese_medicine, 0) as is_chinese_medicine,
        c.indtlid AS sourceid,
        0 AS is_out, -- 入库
        a.ecode
    FROM ods_wms.wms_ecode_record a
    JOIN ods_wms.wms_in_order_dtl c ON a.sourceid = c.indtlid 
    JOIN ods_wms.wms_in_order b ON b.inid = c.inid
    LEFT JOIN ods_wms.tpl_warehouse d ON b.warehid = d.warehid AND d.is_active = 1
    LEFT JOIN ods_wms.tpl_goodsowner j ON b.goodsownerid = j.goodsownerid AND j.is_active = 1
    LEFT JOIN dim.goods e ON c.goodsid = e.goodsid AND a.credate >= e.dw_starttime AND a.credate < e.dw_endtime
    LEFT JOIN dim.wms_goods_feature f ON b.warehid = f.warehid AND c.goodsid = f.goodsid AND a.credate >= f.dw_starttime AND a.credate < f.dw_endtime
    WHERE a.inoutflag = 0 -- 入库采集
        AND IFNULL(a.comefrom, 0) = 1
        AND a.is_active = 1
        AND b.is_active = 1
        AND c.is_active = 1
        AND a.dw_updatetime > (SELECT MAX(dw_updatetime) FROM dwd.logistics_warehouse_ecode)
)
SELECT 
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
    ecode
FROM ecode_data
WHERE recordid IS NOT NULL;