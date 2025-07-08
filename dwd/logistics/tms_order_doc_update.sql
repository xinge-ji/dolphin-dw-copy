INSERT INTO dwd.logistics_tms_order_doc (orderid, __DORIS_DELETE_SIGN__)
SELECT a.orderid, 1
FROM (SELECT orderid, dw_updatetime FROM ods_tms.mlogisticsorder WHERE is_active = 0
      UNION ALL
      SELECT orderid, dw_updatetime FROM ods_tms.mlogisticsorderhistory WHERE is_active = 0) AS a
JOIN dwd.logistics_tms_order_doc AS b ON a.orderid = b.orderid
WHERE a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.logistics_tms_order_doc (
    orderid,
    dw_updatetime,
    create_time,
    create_order_time,
    load_time,
    sign_time,
    goodspeerno
)
SELECT
    orderid,
    dw_updatetime,
    createdtime AS create_time,
    createOrderTime AS create_order_time,
    loadtime AS load_time,
    signtime AS sign_time,
    goodspeerno
FROM
    ods_tms.mlogisticsorder
WHERE is_active = 1 AND dw_updatetime >= (SELECT MAX(dw_updatetime) FROM dwd.logistics_tms_order_doc)

UNION

SELECT
    orderid,
    dw_updatetime,
    createdtime AS create_time,
    createOrderTime AS create_order_time,
    loadtime AS load_time,
    signtime AS sign_time,
    goodspeerno
FROM
    ods_tms.mlogisticsorderhistory
WHERE is_active = 1 AND dw_updatetime >= (SELECT MAX(dw_updatetime) FROM dwd.logistics_tms_order_doc);