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
        usestatus_name,
        is_autotask,
        operationtype,
        operationtype_name
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
    CASE
        WHEN a.usestatus = 0 THEN '取消'
        WHEN a.usestatus = 1 THEN '下单'
        WHEN a.usestatus = 2 THEN '处理中'
        WHEN a.usestatus = 3 THEN '完成'
        WHEN a.usestatus = 4 THEN '挂起'
        ELSE '未定义'
    END AS usestatus_name,
    IFNULL(a.autotaskflag, 0) as is_autotask
    a.operationtype,
    CASE
        WHEN a.operationtype = 1 THEN '进货'
        WHEN a.operationtype = 7 THEN '销退'
        WHEN a.operationtype = 8 THEN '移库入'
        WHEN a.operationtype = 18 THEN '移库出'
        WHEN a.operationtype = 22 THEN '报溢'
        WHEN a.operationtype = 23 THEN '赠品入库'
        WHEN a.operationtype = 25 THEN '赠品出库'
        WHEN a.operationtype = 42 THEN '收配退'
        WHEN a.operationtype = 71 THEN '产成品入库进货(中药饮片业务类型)'
        WHEN a.operationtype = 75 THEN '退料入库(中药饮片业务类型)'
        ELSE '未定义'
    END AS operationtype_name
FROM
    ods_wms.wms_in_order a
    LEFT JOIN ods_wms.tpl_warehouse b ON a.warehid = b.warehid
    AND b.is_active = 1
    LEFT JOIN ods_wms.tpl_goodsowner c ON a.goodsownerid = c.goodsownerid
    AND c.is_active = 1
WHERE 
    a.is_active = 1
    AND a.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_order_in_doc);