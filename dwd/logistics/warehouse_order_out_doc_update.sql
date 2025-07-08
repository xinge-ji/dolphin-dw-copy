INSERT INTO dwd.logistics_warehouse_order_out_doc (outid, __DORIS_DELETE_SIGN__)
SELECT a.outid, 1
FROM ods_wms.wms_out_order AS a
JOIN dwd.logistics_warehouse_order_out_doc AS b ON a.outid = b.outid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.logistics_warehouse_order_out_doc (
    outid,
    dw_updatetime,
    goodsownerid,
    warehid,
    waveid,
    wavedtlid,
    togoodsownerid,
    create_time,
    is_eshop,
    is_autopass,
    is_passing,
    use_status,
    operation_type,
    outmode,
    deptno,
    dept_name
)
SELECT 
    a.outid,
    a.dw_updatetime,
    a.goodsownerid,
    a.warehid,
    a.waveid,
    a.wavedtlid,
    a.togoodsownerid,
    a.credate as create_time,
    IFNULL(a.ysorderflag, 0) as is_eshop,
    IFNULL(a.autopassflag, 0) as is_autopass,
    IFNULL(a.passingflag, 0) as is_passing,
    CASE 
        WHEN a.usestatus = -1 THEN '库存不够'
        WHEN a.usestatus = 0 THEN '取消'
        WHEN a.usestatus = 1 THEN '下单'
        WHEN a.usestatus = 2 THEN '待拣货'
        WHEN a.usestatus = 3 THEN '待复核'
        WHEN a.usestatus = 4 THEN '已出货'
        WHEN a.usestatus = 5 THEN '已抵达'
        WHEN a.usestatus = 6 THEN '等待补货'
        WHEN a.usestatus = 7 THEN '出库取消'
        ELSE '其他'
    END as use_status,
    s.ddlname as operation_type,
    CASE IFNULL(a.outmode, 0)
        WHEN 1 THEN '送货'
        WHEN 0 THEN '自提'
        ELSE '其他'
    END as outmode,
    a.deptno,
    IFNULL(a.deptname, '其他') as dept_name
FROM ods_wms.wms_out_order a
LEFT JOIN ods_wms.sys_ddl_dtl s ON a.operationtype = s.ddlid AND s.sysid = 389 AND s.is_active = 1
WHERE a.is_active = 1
AND a.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_order_out_doc);
