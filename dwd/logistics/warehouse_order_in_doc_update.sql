INSERT INTO dwd.logistics_warehouse_order_in_doc (inid, __DORIS_DELETE_SIGN__)
SELECT a.inid, 1
FROM ods_wms.wms_in_order AS a
JOIN dwd.logistics_warehouse_order_in_doc AS b ON a.inid = b.inid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO
    dwd.logistics_warehouse_order_in_doc (
        -- 主键标识
        inid,
        dw_updatetime,
        -- 时间
        create_time,
        -- 仓库
        warehid,
        warehouse_name,
        -- 货主
        goodsownerid,
        goodsowner_name,
        -- 状态
        usestatus,
        is_autotask
    )
SELECT
    a.inid,
    a.dw_updatetime,
    a.credate as create_time,
    a.warehid,
    b.warehname,
    a.goodsownerid,
    c.goodsownername,
    a.usestatus,
    IFNULL(a.autotaskflag, 0) as is_autotask
FROM
    ods_wms.wms_in_order a
    LEFT JOIN ods_wms.tpl_warehouse b ON a.warehid = b.warehid
    AND b.is_active = 1
    LEFT JOIN ods_wms.tpl_goodsowner c ON a.goodsownerid = c.goodsownerid
    AND c.is_active = 1
WHERE 
    a.is_active = 1
    AND a.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_order_in_doc);