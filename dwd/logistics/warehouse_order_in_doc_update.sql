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
        use_status,
        is_autotask,
        operation_type
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
    END AS use_status,
    IFNULL(a.autotaskflag, 0) as is_autotask,
    IFNULL(s.ddlname, '其他') as operation_type
FROM
    ods_wms.wms_in_order a
    LEFT JOIN ods_wms.tpl_warehouse b ON a.warehid = b.warehid
    AND b.is_active = 1
    LEFT JOIN ods_wms.tpl_goodsowner c ON a.goodsownerid = c.goodsownerid
    AND c.is_active = 1
    LEFT JOIN ods_wms.sys_ddl_dtl s ON a.operationtype = s.ddlid AND s.sysid = 389 AND s.is_active = 1
WHERE
    a.is_active = 1
    AND a.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_order_in_doc);