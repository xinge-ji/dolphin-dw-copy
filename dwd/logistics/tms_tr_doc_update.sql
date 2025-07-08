INSERT INTO dwd.logistics_tms_tr_doc (trid, __DORIS_DELETE_SIGN__)
SELECT a.trid, 1
FROM ods_wms.TMS_luyan_TR_DOC AS a
JOIN dwd.logistics_tms_tr_doc AS b ON a.trid = b.trid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.logistics_tms_tr_doc (
    trid,
    dw_updatetime,
    sign_time,
    warehid,
    goodsownerid,
    goodsowner_name,
    company_name,
    dispatchid,
    goodspeerno
)
SELECT
    trid,
    dw_updatetime,
    signdate AS sign_time,
    warehid,
    goodsownerid,
    goodsownername AS goodsowner_name,
    companyname AS company_name,
    dispatchid,
    sourceid as goodspeerno
FROM
    ods_wms.TMS_luyan_TR_DOC
WHERE is_active = 1 AND dw_updatetime >= (SELECT MAX(dw_updatetime) FROM dwd.logistics_tms_tr_doc);