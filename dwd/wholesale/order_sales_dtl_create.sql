DROP TABLE IF EXISTS dwd.wholesale_order_sales_dtl;
CREATE TABLE dwd.wholesale_order_sales_dtl (
    -- 颗粒度
	salesid bigint COMMENT '销售单ID',
    salesdtlid bigint COMMENT '销售单明细ID',

    -- 基础信息
    dw_updatetime datetime COMMENT '数据更新时间',

    -- 时间维度
    create_date datetime COMMENT '创建日期',
    confirm_date datetime COMMENT '确认日期',
    yewu_date datetime COMMENT '业务日期',
    
    -- 组织维度
    entryid bigint COMMENT '独立单元ID',
    entry_name varchar COMMENT '独立单元名称',
    province_name varchar COMMENT '省份名称',
    city_name varchar COMMENT '城市名称',
    area_name varchar COMMENT '区域名称',
        
    -- 客户维度
    customid bigint COMMENT '客户ID',
    customer_name varchar COMMENT '客户名称',
    customertype_name varchar COMMENT '客户类型',
    customertype_group varchar COMMENT '客户类型组',

    -- 业务部门
    salesdeptid bigint COMMENT '业务部门ID',
    sales_dept_name varchar COMMENT '业务部门名称',

    -- 订单状态
    use_status varchar COMMENT '使用状态:正式/临时',
    dtl_memo varchar COMMENT '订单细单备注',

    -- 销售模式
    peihuojiekuan_mode varchar COMMENT '配货结款模式',
    sale_type varchar COMMENT '销售类型:销售/销退',
    sale_mode varchar COMMENT '销售方式:普通销售/委托销售/寄售',
    
    -- 结算相关
    settletypeid bigint COMMENT '结算类型ID',
    settle_type varchar COMMENT '结算类型',
    credit_approve_status varchar COMMENT '信用审批状态',
    
    -- 人员维度
    salerid bigint COMMENT '销售员ID',
    saler_name varchar COMMENT '销售员名称',
    inputmanid bigint COMMENT '制单人ID',
    inputman_name varchar COMMENT '制单人名称',
    
    -- 来源相关
    comefrom varchar COMMENT '订单来源',
    econid bigint COMMENT '云商订单ID',
    is_btob tinyint COMMENT '是否云商订单',
    pt_orderid decimal(38,18) COMMENT '海西订单号',
    is_haixi tinyint COMMENT '是否海西订单',
    cgdbh varchar COMMENT '药械网单号',
    is_yaoxiewang tinyint COMMENT '是否药械网订单',
    ysbdjbh bigint COMMENT '药师帮订单编号',
    is_yaoshibang tinyint COMMENT '是否药师帮订单',
    is_dianshang tinyint COMMENT '是否电商(云商/海西/药械网/药师帮)订单',

    -- 商品类型
    goodsid bigint COMMENT '商品ID',
    goods_name varchar COMMENT '商品名称',
    nianbao_type varchar COMMENT '年报类型',
    leibiebeizhu int COMMENT '类别备注',

    -- 集采相关
    is_jicai_zhongxuan tinyint COMMENT '是否集采',
    jicai_liangneiwai varchar COMMENT '集采量内量外',
    jicai_type varchar COMMENT '非集采/集采/量内/量外',

    -- 订单相关
    goods_qty decimal(16,6) COMMENT '数量',
    unit_price decimal(20,10) COMMENT '单价',
    sales_amount decimal(18,4) COMMENT '销售额',
    settle_status varchar COMMENT '结算状态:不结算/需要结算',
    priceid bigint COMMENT '价格ID',
    price_name varchar COMMENT '价格名称',
    tax_rate decimal(3,2) COMMENT '税率',
    lastunit_price decimal(20,10) COMMENT '上次单价',
    discount decimal(18,4) COMMENT '折扣',
    lastpriceid bigint COMMENT '上次价格ID',
    reference_price decimal(18,4) COMMENT '参考价',
    sales_gross_profit float COMMENT '销售额毛利(销售额-批次成本)',
    sales_gross_profit_rate float COMMENT '销售额毛利率(销售额-批次成本)/销售额',

    -- 仓储相关
    storageid bigint COMMENT '保管帐ID',
    storage_name varchar COMMENT '保管帐名称',

    -- 批次相关
    batchid bigint COMMENT '批次ID',
    batch_notax_price decimal(18, 4) COMMENT '批次不含税成本价格',
    batch_price decimal(18,4) COMMENT '批次成本价格',
    reference_price_type varchar COMMENT '参考价类型',
    supplier_company_name varchar COMMENT '供应商公司名称'
)
UNIQUE KEY(salesid, salesdtlid) DISTRIBUTED BY HASH(salesid, salesdtlid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 插入数据
INSERT INTO dwd.wholesale_order_sales_dtl (
    -- 颗粒度
    salesid,
    salesdtlid,
    
    -- 基础信息
    dw_updatetime,
    
    -- 时间维度
    create_date,
    confirm_date,
    yewu_date,
    
    -- 组织维度
    entryid,
    entry_name,
    province_name,
    city_name,
    area_name,
    
    -- 客户维度
    customid,
    customer_name,
    customertype_name,
    customertype_group,

    -- 业务部门
    salesdeptid,
    sales_dept_name,
    
    -- 订单状态
    use_status,
    dtl_memo,
    
    -- 销售模式
    peihuojiekuan_mode,
    sale_type,
    sale_mode,
    
    -- 结算相关
    settletypeid,
    settle_type,
    credit_approve_status,
    
    -- 人员维度
    salerid,
    saler_name,
    inputmanid,
    inputman_name,
    
    -- 来源相关
    comefrom,
    econid,
    is_btob,
    pt_orderid,
    is_haixi,
    cgdbh,
    is_yaoxiewang,
    ysbdjbh,
    is_yaoshibang,
    is_dianshang,
    
    -- 商品类型
    goodsid,
    goods_name,
    nianbao_type,
    leibiebeizhu,
    
    -- 集采相关
    is_jicai_zhongxuan,
    jicai_liangneiwai,
    jicai_type,
    
    -- 订单相关
    goods_qty,
    unit_price,
    sales_amount,
    settle_status,
    priceid,
    price_name,
    tax_rate,
    lastunit_price,
    discount,
    lastpriceid,
    reference_price,
    sales_gross_profit,
    sales_gross_profit_rate,
    
    -- 仓储相关
    storageid,
    storage_name,
    
    -- 批次相关
    batchid,
    batch_notax_price,
    batch_price,
    reference_price_type,
    supplier_company_name
)
SELECT
    -- 颗粒度
    a.salesid,
    b.salesdtlid,
    
    -- 基础信息
    b.dw_updatetime,
    
    -- 时间维度
    a.create_date,
    a.confirm_date,
    CASE
        WHEN g.nianbao_type = '医疗器械' THEN a.confirm_date
        ELSE a.create_date
    END AS yewu_date,
    
    -- 组织维度
    a.entryid,
    a.entry_name,
    a.province_name,
    a.city_name,
    a.area_name,
    
    -- 客户维度
    a.customid,
    a.customer_name,
    a.customertype_name,
    a.customertype_group,

    -- 业务部门
    a.salesdeptid,
    a.sales_dept_name,
    
    -- 订单状态
    a.use_status,
    b.dtlmemo as dtl_memo,
    
    -- 销售模式
    a.peihuojiekuan_mode,
    a.sale_type,
    a.sale_mode,
    
    -- 结算相关
    a.settletypeid,
    a.settle_type,
    a.credit_approve_status,
    
    -- 人员维度
    a.salerid,
    a.saler_name,
    a.inputmanid,
    a.inputman_name,
    
    -- 来源相关
    a.comefrom,
    a.econid,
    IFNULL(a.is_btob, 0),
    a.pt_orderid,
    IFNULL(a.is_haixi, 0),
    a.cgdbh,
    IFNULL(a.is_yaoxiewang, 0),
    wcd.ysbdjbh,
    CASE WHEN wcd.ysbdjbh IS NOT NULL THEN 1 ELSE 0 END,
    CASE 
        WHEN IFNULL(a.is_btob, 0) = 1 
             OR IFNULL(a.is_haixi, 0) = 1 
             OR IFNULL(a.is_yaoxiewang, 0) = 1 
             OR wcd.ysbdjbh IS NOT NULL 
        THEN 1 
        ELSE 0 
    END AS is_dianshang,
    
    -- 商品类型
    b.goodsid,
    g.goods_name,
    g.nianbao_type,
    IFNULL(b.zx_lbbz, -1) AS leibiebeizhu,
    
    -- 集采相关
    IFNULL(b.zx_jczxflag, 0) AS is_jicai_zhongxuan,
    j.jicai_liangneiliangwai,
    CASE
        WHEN a.province_name = '福建省' THEN '非集采'
        WHEN a.province_name IN ('江西省', '海南省') AND (IFNULL(b.zx_jczxflag, 0) = 1 OR j.jicai_liangneiliangwai is not null) THEN '集采'
        WHEN a.province_name = '四川省' AND j.jicai_liangneiliangwai = '量内' THEN '集采量内'
        WHEN a.province_name = '四川省' AND j.jicai_liangneiliangwai = '量外' THEN '集采量外'
        ELSE '非集采'
    END AS jicai_type,
    
    -- 订单相关
    b.goodsqty,
    b.unitprice,
    b.total_line,
    CASE
        WHEN IFNULL(b.settleflag, 0) = 2 THEN '不结算'
        ELSE '需要结算'
    END AS settle_status,
    b.priceid,
    pt1.price_name,
    b.taxrate,
    b.lastunitprice,
    b.discount,
    b.lastpriceid,
    b.timeprice,
    IFNULL(ROUND((b.unitprice - batch.unit_price) * b.goodsqty, 2), 0) AS sales_gross_profit,
    IFNULL(ROUND(
        CASE 
            WHEN b.unitprice = 0 THEN 0 
            ELSE (b.unitprice - batch.unit_price) / b.unitprice 
        END, 4), 0) AS sales_gross_profit_rate,
    
    -- 仓储相关
    b.storageid,
    s.storage_name,
    
    -- 批次相关
    b.batchid,
    batch.notax_price AS batch_notax_price,
    batch.unit_price AS batch_price,
    pt2.price_name AS reference_price_type,
    batch.company_name AS supplier_company_name
FROM
    dwd.wholesale_order_sales_doc a
JOIN
    ods_erp.bms_sa_dtl b ON a.salesid = b.salesid
LEFT JOIN
    dim.goods g ON b.goodsid = g.goodsid 
    AND a.create_date >= g.dw_starttime AND a.create_date < g.dw_endtime
LEFT JOIN
    dim.price_type pt1 ON b.priceid = pt1.priceid 
    AND a.create_date >= pt1.dw_starttime AND a.create_date < pt1.dw_endtime
LEFT JOIN
    dim.price_type pt2 ON b.lastpriceid = pt2.priceid 
    AND a.create_date >= pt2.dw_starttime AND a.create_date < pt2.dw_endtime
LEFT JOIN
    dim.storage s ON b.storageid = s.storageid 
    AND a.create_date >= s.dw_starttime AND a.create_date < s.dw_endtime
LEFT JOIN
    dwd.wholesale_jicai_volume_dtl j ON b.salesdtlid = j.salesdtlid
LEFT JOIN
    dim.batch batch ON b.batchid = batch.batchid 
    AND a.create_date >= batch.dw_starttime AND a.create_date < batch.dw_endtime
LEFT JOIN
    dwd.wholesale_contract_dtl wcd ON b.salesdtlid = wcd.salesdtlid
WHERE 
    b.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_sales_dtl_salesid ON dwd.wholesale_order_sales_dtl (salesid);
CREATE INDEX IF NOT EXISTS idx_sales_dtl_salesdtlid ON dwd.wholesale_order_sales_dtl (salesdtlid);
CREATE INDEX IF NOT EXISTS idx_sales_dtl_entryid ON dwd.wholesale_order_sales_dtl (entryid);
CREATE INDEX IF NOT EXISTS idx_sales_dtl_customid ON dwd.wholesale_order_sales_dtl (customid);
CREATE INDEX IF NOT EXISTS idx_sales_dtl_goodsid ON dwd.wholesale_order_sales_dtl (goodsid);
CREATE INDEX IF NOT EXISTS idx_sales_dtl_batchid ON dwd.wholesale_order_sales_dtl (batchid);