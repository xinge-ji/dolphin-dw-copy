INSERT INTO dwd.logistics_warehouse_pick_doc (pickid, __DORIS_DELETE_SIGN__)
SELECT a.pickid, 1
FROM ods_wms.wms_st_io_doc AS a
JOIN dwd.logistics_warehouse_pick_doc AS b ON a.pickid = b.inoutid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.logistics_warehouse_pick_doc (
    pickid,
    dw_updatetime,
    create_time,
    pick_time,
    check_time,
    pickareaid,
    pickarea_name,
    taskid,
    task_type,
    io_comefrom,
    wavedtlid,
    ssc_picking_carton_id,
    is_iwcs,
    rfflag,
    usestatus,
    goodsid,
    operation_type,
    goods_name,
    goods_type,
    goods_qty,
    tradepackname,
    lotno,
    pack_name,
    pack_size,
    customid,
    customer_name,
    product_area,
    factory_name,
    drug_form,
    drug_form_no,
    pickflowid,
    pickflow_name,
    groupid,
    group_name,
    is_autotask,
    rfmanid,
    rfman_name,
    checkmanid,
    checkman_name,
    inoutid,
    warehid,
    posid,
    whole_qty,
    scatter_qty,
    goodsownerid,
    goodsowner_name,
    case_qty,
    areaflag,
    goods_category
)
SELECT 
    -- 出库下架任务明细
    b.inoutid as pickid,  -- pickid与inoutid一致
    b.dw_updatetime,
    b.credate as create_time,
    b.rffindate as pick_time,
    b.keepdate as check_time,
    f.pickareaid,
    g.pickareaname as pickarea_name,
    a.taskid,
    '出库下架' as task_type,
    CASE
        WHEN b.comefrom = 2 THEN '订单出库'
        WHEN b.comefrom = 3 THEN '波次出库'
    END as io_comefrom,
    v.wavedtlid,
    u.ssc_picking_carton_id,
    IFNULL(p.iwcs_flag, 0) as is_iwcs,
    b.rfflag,
    b.usestatus,
    b.goodsid,
    s.ddlname as operation_type,
    j.goodsname as goods_name,
    j.goodstype as goods_type,
    b.goodsqty as goods_qty,
    j.tradepackname,
    i.lotno,
    h.packname as pack_name,
    h.packsize as pack_size,
    d.customid,
    k.companyname as customer_name,
    j.prodarea as product_area,
    j.factname as factory_name,
    j.drugform as drug_form,
    l.drugformno as drug_form_no,
    m.pickflowid,
    m.pickflowname as pickflow_name,
    n.groupid,
    n.groupname as group_name,
    IFNULL(b.autotaskflag, 0) as is_autotask,
    b.rfmanid,
    o.employeename as rfman_name,
    b.keepmanid as checkmanid,
    r.employeename as checkman_name,
    b.inoutid,
    b.warehid,
    b.posid,
    b.wholeqty as whole_qty,
    b.scatterqty as scatter_qty,
    j.goodsownerid,
    t.goodsownername as goodsowner_name,
    CASE WHEN IFNULL(h.packsize,0) = 0 THEN 0 ELSE CEIL(b.goodsqty/h.packsize) END as case_qty,
    q.areaflag,
    w.goods_category
FROM ods_wms.wms_task_doc a
INNER JOIN ods_wms.wms_st_io_doc b ON a.taskid = b.taskid AND b.is_active = 1
LEFT JOIN ods_wms.wms_wave_goods_dtl c ON b.sourceid = c.wavegoodsdtlid AND c.is_active = 1
LEFT JOIN ods_wms.wms_wave_dtl d ON c.wavedtlid = d.wavedtlid AND d.is_active = 1
LEFT JOIN ods_wms.wms_wave_doc e ON d.waveid = e.wavedocid AND e.is_active = 1
LEFT JOIN ods_wms.wms_pickarea_pos_def f ON b.posid = f.posid AND f.is_active = 1
LEFT JOIN ods_wms.wms_pickarea g ON f.pickareaid = g.pickareaid AND g.is_active = 1
LEFT JOIN ods_wms.tpl_pub_goods_packs h ON b.goodspackid = h.goodspackid ANd h.is_active = 1
LEFT JOIN ods_wms.wms_goods_lot i ON b.lotid = i.lotid AND i.is_active = 1
LEFT JOIN ods_wms.tpl_goods j ON b.ownergoodsid = j.ownergoodsid AND j.is_active = 1
LEFT JOIN ods_wms.tpl_go_company k ON d.customid = k.companyid AND k.is_active = 1
LEFT JOIN ods_wms.tpl_drugform l ON j.drugform = l.drugformno AND l.is_active = 1
LEFT JOIN ods_wms.wms_pickflow m ON g.pickflowid = m.pickflowid AND m.is_active = 1
LEFT JOIN ods_wms.wms_group_def n ON g.groupid = n.groupid AND n.is_active = 1
LEFT JOIN ods_wms.pub_employee o ON b.rfmanid = o.employeeid AND o.is_active = 1
LEFT JOIN ods_wms.wms_st_section_def p ON b.sectionid = p.sectionid AND p.is_active = 1
LEFT JOIN ods_wms.wms_st_area_def q ON p.areaid = q.areaid AND q.is_active = 1
LEFT JOIN ods_wms.pub_employee r ON b.keepmanid = r.employeeid AND r.is_active = 1
LEFT JOIN ods_wms.sys_ddl_dtl s ON b.operationtype = s.ddlid AND s.sysid = 389 AND s.is_active = 1
LEFT JOIN ods_wms.tpl_goodsowner t ON j.goodsownerid = t.goodsownerid AND t.is_active = 1
LEFT JOIN ods_wms.iwcs_ssc_picking_carton u ON b.inoutid = u.wms_inout_id
LEFT JOIN ods_wms.wms_wave_goods_dtl v ON b.sourceid = v.wavegoodsdtlid AND v.is_active = 1
LEFT JOIN dim.wms_goods_feature w ON w.warehid = b.warehid AND w.goodsid = b.goodsid AND b.credate >= w.dw_starttime AND b.credate < w.dw_endtime
WHERE a.tasktype = 1  -- 拣货任务（波次生成）
  AND b.comefrom in (2, 3)  -- 波次出库
  AND a.is_active = 1
  AND b.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_pick_doc.dw_updatetime)

UNION ALL

SELECT 
    -- 补货下架任务明细
    b.inoutid as pickid,  -- pickid与inoutid一致
    b.dw_updatetime,
    b.credate as create_time,
    b.rffindate as pick_time,
    b.keepdate as check_time,
    f.pickareaid,
    g.pickareaname as pickarea_name,
    a.taskid,
    CASE e.subtype
        WHEN 1 THEN '波次补货下架'
        WHEN 5 THEN '波次预补货下架'
        WHEN 4 THEN '手工补货下架'
        WHEN 3 THEN '闲时补货下架'
        ELSE '报警补货下架'
    END as task_type,
    '库内变动' as io_comefrom,
    v.wavedtlid,
    u.ssc_picking_carton_id,
    IFNULL(p.iwcs_flag, 0) as is_iwcs,
    b.rfflag,
    b.usestatus,
    b.goodsid,
    s.ddlname as operation_type,
    j.goodsname as goods_name,
    j.goodstype as goods_type,
    b.goodsqty as goods_qty,
    j.tradepackname,
    i.lotno,
    h.packname as pack_name,
    h.packsize as pack_size,
    NULL as customid,
    NULL as customer_name,
    j.prodarea as product_area,
    j.factname as factory_name,
    l.drugform as drug_form,
    l.drugformno as drug_form_no,
    m.pickflowid,
    m.pickflowname as pickflow_name,
    n.groupid,
    n.groupname as group_name,
    IFNULL(b.autotaskflag, 0) as is_autotask,
    b.rfmanid,
    o.employeename as rfman_name,
    b.keepmanid as checkmanid,
    r.employeename as checkman_name,
    b.inoutid,
    b.warehid,
    b.posid,
    b.wholeqty as whole_qty,
    b.scatterqty as scatter_qty,
    j.goodsownerid,
    t.goodsownername as goodsowner_name,
    CASE WHEN IFNULL(h.packsize,0) = 0 THEN 0 ELSE CEIL(b.goodsqty/h.packsize) END as case_qty,
    q.areaflag,
    w.goods_category
FROM ods_wms.wms_task_doc a
INNER JOIN ods_wms.wms_st_io_doc b ON a.taskid = b.taskid AND b.is_active = 1
LEFT JOIN ods_wms.wms_trade_dtl d ON b.sourceid = d.tradedtlid AND d.is_active = 1
LEFT JOIN ods_wms.wms_trade_order e ON d.tradeid = e.tradeid AND e.is_active = 1
LEFT JOIN ods_wms.wms_pickarea_pos_def f ON b.posid = f.posid AND f.is_active = 1
LEFT JOIN ods_wms.wms_pickarea g ON f.pickareaid = g.pickareaid AND g.is_active = 1
LEFT JOIN ods_wms.tpl_pub_goods_packs h ON b.goodspackid = h.goodspackid AND h.is_active = 1
LEFT JOIN ods_wms.wms_goods_lot i ON b.lotid = i.lotid AND i.is_active = 1
LEFT JOIN ods_wms.tpl_goods j ON b.ownergoodsid = j.ownergoodsid AND j.is_active = 1
LEFT JOIN ods_wms.tpl_drugform l ON j.drugform = l.drugformno AND l.is_active = 1
LEFT JOIN ods_wms.wms_pickflow m ON g.pickflowid = m.pickflowid AND m.is_active = 1
LEFT JOIN ods_wms.wms_group_def n ON g.groupid = n.groupid AND n.is_active = 1
LEFT JOIN ods_wms.pub_employee o ON b.rfmanid = o.employeeid AND o.is_active = 1
LEFT JOIN ods_wms.wms_st_section_def p ON b.sectionid = p.sectionid AND p.is_active = 1
LEFT JOIN ods_wms.wms_st_area_def q ON p.areaid = q.areaid AND q.is_active = 1
LEFT JOIN ods_wms.pub_employee r ON b.keepmanid = r.employeeid AND r.is_active = 1
LEFT JOIN ods_wms.sys_ddl_dtl s ON b.operationtype = s.ddlid AND s.sysid = 389 AND s.is_active = 1
LEFT JOIN ods_wms.tpl_goodsowner t ON j.goodsownerid = t.goodsownerid AND t.is_active = 1
LEFT JOIN ods_wms.iwcs_ssc_picking_carton u ON b.inoutid = u.wms_inout_id
LEFT JOIN ods_wms.wms_wave_goods_dtl v ON b.sourceid = v.wavegoodsdtlid AND v.is_active = 1
LEFT JOIN dim.wms_goods_feature w ON w.warehid = b.warehid AND w.goodsid = b.goodsid AND b.credate >= w.dw_starttime AND b.credate < w.dw_endtime
WHERE a.tasktype = 2  -- 拣货任务（波次生成）
  AND b.comefrom = 4  -- 库内变动
  AND e.subtype IN (1, 2, 3, 4, 5)  -- 1 波次补货 2 报警补货 3 闲时补货 4手工补货 5波次预补货
  AND a.is_active = 1
  AND b.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_pick_doc.dw_updatetime);