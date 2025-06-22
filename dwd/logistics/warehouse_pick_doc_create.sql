DROP TABLE IF EXISTS dwd.logistics_warehouse_pick_doc;

CREATE TABLE dwd.logistics_warehouse_pick_doc (
    -- 主键
    pickid bigint COMMENT '拣货单ID(=inoutid)',

    -- 数据更新时间
    dw_updatetime datetime COMMENT '更新时间',

    -- 时间字段
    create_time datetime COMMENT '任务下达时间',
    pick_time datetime COMMENT '拣货时间',
    check_time datetime COMMENT '复核时间',
    
    -- 拣货工作区信息
    pickareaid bigint COMMENT '拣货工作区ID',
    pickarea_name varchar COMMENT '拣货工作区名称',
    
    -- 任务信息
    operation_type varchar COMMENT '操作类型',
    taskid bigint COMMENT '任务ID(1对多拣货单ID)',
    task_type varchar COMMENT '任务类型',
    io_comefrom varchar COMMENT '来源',
    wavedtlid bigint COMMENT '波次细单ID',
    ssc_picking_carton_id bigint COMMENT 'iwcs拣货ID',
    is_iwcs tinyint COMMENT '是否iwcs分区',
    
    -- 状态信息
    rfflag int COMMENT '标志',
    usestatus varchar COMMENT '使用状态',
    
    -- 货品信息
    goodsid bigint COMMENT '货品ID',
    goods_name varchar COMMENT '货品名称',
    goods_type varchar COMMENT '货品规格',
    goods_qty decimal(18,4) COMMENT '数量',
    tradepackname varchar COMMENT '单位',
    
    -- 批次和包装信息
    lotno varchar COMMENT '批号',
    pack_name varchar COMMENT '包装单位',
    pack_size decimal(18,4) COMMENT '包装大小',
    
    -- 客户信息
    customid bigint COMMENT '客户ID',
    customer_name varchar COMMENT '客户名称',
    
    -- 产品详细信息
    product_area varchar COMMENT '产地',
    factory_name varchar COMMENT '厂家名称',
    drug_form varchar COMMENT '剂型',
    drug_form_no varchar COMMENT '剂型编号',
    
    -- 流程信息
    pickflowid bigint COMMENT '拣货流程ID',
    pickflow_name varchar COMMENT '拣货流程名称',
    groupid bigint COMMENT '组ID',
    group_name varchar COMMENT '组名称',
    is_autotask tinyint COMMENT '是否自动任务',
    
    -- 操作人员
    rfmanid bigint COMMENT '拣货操作员ID',
    rfman_name varchar COMMENT '拣货操作员姓名',
    checkmanid bigint COMMENT '复核操作员ID',
    checkman_name varchar COMMENT '复核操作员姓名',
    
    -- 其他信息
    inoutid bigint COMMENT '出入库单ID',
    warehid bigint COMMENT '仓库ID',
    posid bigint COMMENT '货位ID',
    whole_qty decimal(18,4) COMMENT '整件数量',
    scatter_qty decimal(18,4) COMMENT '散件数量',
    goodsownerid bigint COMMENT '货主ID',
    goodsowner_name varchar COMMENT '货主名称',
    case_qty decimal(18,0) COMMENT '件数',
    areaflag varchar COMMENT '存放类型',
    goods_category varchar COMMENT '商品分类:冷链/中药/其他'
)
UNIQUE KEY(pickid)
DISTRIBUTED BY HASH(pickid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
);

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
  AND a.is_active = 1;