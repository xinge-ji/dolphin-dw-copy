INSERT INTO dwd.logistics_warehouse_shelf_doc (shelfid, __DORIS_DELETE_SIGN__)
SELECT a.shelfid, 1
FROM ods_wms.wms_st_io_doc AS a
JOIN dwd.logistics_warehouse_shelf_doc AS b ON a.shelfid = b.inoutid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.logistics_warehouse_shelf_doc (
    shelfid,
    dw_updatetime,
    create_time,
    shelf_time,
    sectionid,
    section_name,
    operation_type,
    taskid,
    task_type,
    rfflag,
    usestatus,
    io_comefrom,
    is_iwcs,
    goodsid,
    originalgoodsid,
    goods_name,
    goods_type,
    factory_name,
    goods_qty,
    trade_pack_name,
    product_area,
    lotno,
    pack_name,
    pack_size,
    source_company_id,
    source_company_name,
    inoutid,
    drug_form,
    drug_form_no,
    lpn_code,
    checkman_1,
    checkman_2,
    containerid,
    container_no,
    warehouseid,
    warehouse_name,
    goodsownerid,
    goodsowner_name
)
SELECT 
    -- 入库上架任务明细
    b.inoutid AS shelfid,
    b.dw_updatetime AS dw_updatetime,
    b.credate AS create_time,
    b.rffindate AS shelf_time,  
    b.sectionid,
    j.sectionname AS section_name,
    s.ddlname as operation_type,
    a.taskid,
    '入库上架' AS task_type,
    b.rfflag,
    b.usestatus,
    CASE b.comefrom
        WHEN 1 THEN '入库'
        WHEN 4 THEN '库内变动'
        ELSE '其他'
    END AS io_comefrom,
    IFNULL(p.iwcs_flag, 0) as is_iwcs,
    b.goodsid,
    g.goodsownid AS originalgoodsid,
    g.goodsname AS goods_name,
    g.goodstype AS goods_type,
    g.factname AS factory_name,
    b.goodsqty AS goods_qty,
    g.tradepackname AS trade_pack_name,
    g.prodarea AS product_area,
    i.lotno,
    h.packname AS pack_name,
    h.packsize AS pack_size,
    e.sourcecompanyid AS source_company_id,
    f.companyname AS source_company_name,
    b.inoutid,
    k.drugform AS drug_form,
    k.drugformno AS drug_form_no,
    c.lpncode AS lpn_code,
    c.checkman1 AS checkman_1,
    c.checkman2 AS checkman_2,
    b.containerid,
    t.containerno AS container_no,
    a.warehid AS warehouseid,
    l.warehname AS warehouse_name,
    g.goodsownerid,
    m.goodsownername AS goodsowner_name
FROM ods_wms.wms_task_doc a
JOIN ods_wms.wms_st_io_doc b ON a.taskid = b.taskid
JOIN ods_wms.wms_receive_dtl c ON b.sourceid = c.receiveid
JOIN ods_wms.wms_in_order_dtl d ON c.indtlid = d.indtlid
JOIN ods_wms.wms_in_order e ON d.inid = e.inid
LEFT JOIN ods_wms.tpl_go_company f ON e.sourcecompanyid = f.companyid
JOIN ods_wms.tpl_goods g ON b.ownergoodsid = g.ownergoodsid
LEFT JOIN ods_wms.tpl_pub_goods_packs h ON b.goodspackid = h.goodspackid
LEFT JOIN ods_wms.wms_goods_lot i ON b.lotid = i.lotid
LEFT JOIN ods_wms.wms_st_section_def j ON b.sectionid = j.sectionid
LEFT JOIN ods_wms.tpl_drugform k ON g.drugform = k.drugformno
JOIN ods_wms.wms_container_def t ON b.containerid = t.containerid
LEFT JOIN ods_wms.tpl_warehouse l ON a.warehid = l.warehid
LEFT JOIN ods_wms.tpl_goodsowner m ON g.goodsownerid = m.goodsownerid
LEFT JOIN ods_wms.wms_st_section_def p ON b.sectionid = p.sectionid AND p.is_active = 1
LEFT JOIN ods_wms.sys_ddl_dtl s ON b.operationtype = s.ddlid AND s.sysid = 389 AND s.is_active = 1
WHERE a.tasktype = 3  -- 上架任务
  AND b.comefrom = 1  -- 入库
  AND b.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_shelf_doc.dw_updatetime)

UNION ALL

SELECT 
    -- 补货上架任务明细
    b.inoutid AS shelfid,
    NOW() AS dw_updatetime,
    b.credate AS create_time,
    b.rffindate AS shelf_time,  
    b.sectionid,
    j.sectionname AS section_name,
    s.ddlname as operation_type,
    a.taskid,
    CASE e.subtype
        WHEN 1 THEN '波次补货上架'
        WHEN 5 THEN '波次预补货上架'
        WHEN 4 THEN '手工补货上架'
        WHEN 3 THEN '闲时补货上架'
        ELSE '报警补货上架'
    END AS task_type,
    b.rfflag,
    b.usestatus,
    CASE b.comefrom
        WHEN 1 THEN '入库'
        WHEN 4 THEN '库内变动'
        ELSE '其他'
    END AS io_comefrom,
    IFNULL(p.iwcs_flag, 0) as is_iwcs,
    b.goodsid,
    g.goodsownid AS originalgoodsid,
    g.goodsname AS goods_name,
    g.goodstype AS goods_type,
    g.factname AS factory_name,
    b.goodsqty AS goods_qty,
    g.tradepackname AS trade_pack_name,
    g.prodarea AS product_area,
    i.lotno,
    h.packname AS pack_name,
    h.packsize AS pack_size,
    NULL AS source_company_id,
    NULL AS source_company_name,
    b.inoutid,
    k.drugform AS drug_form,
    k.drugformno AS drug_form_no,
    NULL AS lpn_code,
    NULL AS checkman_1,
    NULL AS checkman_2,
    b.containerid,
    t.containerno AS container_no,
    a.warehid AS warehouseid,
    l.warehname AS warehouse_name,
    g.goodsownerid,
    m.goodsownername AS goodsowner_name
FROM ods_wms.wms_task_doc a
JOIN ods_wms.wms_st_io_doc b ON a.taskid = b.taskid
JOIN ods_wms.wms_trade_dtl d ON b.sourceid = d.tradedtlid
JOIN ods_wms.wms_trade_order e ON d.tradeid = e.tradeid
LEFT JOIN ods_wms.tpl_goods g ON b.ownergoodsid = g.ownergoodsid
LEFT JOIN ods_wms.tpl_pub_goods_packs h ON b.goodspackid = h.goodspackid
LEFT JOIN ods_wms.wms_goods_lot i ON b.lotid = i.lotid
LEFT JOIN ods_wms.wms_st_section_def j ON b.sectionid = j.sectionid
LEFT JOIN ods_wms.tpl_drugform k ON g.drugform = k.drugformno
JOIN ods_wms.wms_container_def t ON b.containerid = t.containerid
LEFT JOIN ods_wms.tpl_warehouse l ON a.warehid = l.warehid
LEFT JOIN ods_wms.tpl_goodsowner m ON g.goodsownerid = m.goodsownerid
LEFT JOIN ods_wms.wms_st_section_def p ON b.sectionid = p.sectionid AND p.is_active = 1
LEFT JOIN ods_wms.sys_ddl_dtl s ON b.operationtype = s.ddlid AND s.sysid = 389 AND s.is_active = 1
WHERE a.tasktype = 3  -- 上架
  AND b.comefrom = 4  -- 库内下架任务（指库内变动单生成的下架任务）
  AND e.subtype IN (1, 2, 3, 4, 5)  -- 1 波次补货 2 报警补货 3 闲时补货 4手工补货 5波次预补货
  AND b.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_shelf_doc.dw_updatetime)
