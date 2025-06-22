CREATE TABLE dwd.logistics_warehouse_shelf_doc (
    -- 主键
    shelfid bigint COMMENT '上架单ID (=inoutid)',

    -- 数据更新时间
    dw_updatetime datetime COMMENT '数据更新时间',

    -- 时间信息
    create_time datetime COMMENT '任务下达时间',
    shelf_time datetime COMMENT '上架时间',

    -- 分区信息
    sectionid bigint COMMENT '分区ID',
    section_name varchar COMMENT '分区名称',

    -- 任务信息
    operation_type varchar COMMENT '操作类型',
    taskid bigint COMMENT '任务ID (1对多上架单)',
    task_type varchar COMMENT '任务类型',
    rfflag tinyint COMMENT 'RF标志',
    usestatus varchar COMMENT '使用状态',
    io_comefrom varchar COMMENT '来源',
    is_iwcs tinyint COMMENT '是否IWCS分区',

    -- 商品信息
    goodsid bigint COMMENT '商品ID',
    originalgoodsid varchar COMMENT '货主原商品ID',
    goods_name varchar COMMENT '商品名称',
    goods_type varchar COMMENT '商品类型',
    factory_name varchar COMMENT '厂家名称',
    goods_qty decimal(18,4) COMMENT '商品数量',
    trade_pack_name varchar COMMENT '贸易包装名称',
    product_area varchar COMMENT '产地',

    -- 批次和包装信息
    lotno varchar COMMENT '批号',
    pack_name varchar COMMENT '包装名称',
    pack_size decimal(18,4) COMMENT '包装大小',

    -- 公司信息
    source_company_id bigint COMMENT '来源公司ID',
    source_company_name varchar COMMENT '来源公司名称',

    -- 其他信息
    inoutid bigint COMMENT '出入库单ID',
    drug_form varchar COMMENT '剂型',
    drug_form_no varchar COMMENT '剂型编号',
    lpn_code varchar COMMENT 'LPN码',
    checkman_1 varchar COMMENT '检查员1',
    checkman_2 varchar COMMENT '检查员2',
    containerid bigint COMMENT '容器ID',
    container_no varchar COMMENT '容器编号',

    -- 仓库和货主信息
    warehouseid bigint COMMENT '仓库ID',
    warehouse_name varchar COMMENT '仓库名称',
    goodsownerid bigint COMMENT '货主ID',
    goodsowner_name varchar COMMENT '货主名称'
)
UNIQUE KEY (shelfid)
DISTRIBUTED BY HASH (shelfid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
);

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
  AND e.subtype IN (1, 2, 3, 4, 5);  -- 1 波次补货 2 报警补货 3 闲时补货 4手工补货 5波次预补货
