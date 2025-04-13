DROP TABLE IF EXISTS dwd.wholesale_order_settle_dtl;

-- 创建批发订单结算明细表
CREATE TABLE dwd.wholesale_order_settle_dtl (
    -- 主键信息
    sasettleid bigint COMMENT "结算单ID",                  
    sasettledtlid bigint COMMENT "结算单明细ID",

    -- 关联信息
    salesid bigint COMMENT "销售单ID",
    salesdtlid bigint COMMENT "销售单明细ID",
    dw_updatetime datetime COMMENT "数仓数据更新时间",
    
    -- 组织信息
    entryid bigint COMMENT "独立单元ID",
    entry_name varchar COMMENT "独立单元名称",
    province_name varchar COMMENT "独立单元省份名称",
    city_name varchar COMMENT "独立单元城市名称",
    area_name varchar COMMENT "独立单元地区名称",
    caiwu_level1 varchar COMMENT "集团模块一级",
    caiwu_level2 varchar COMMENT "集团模块二级",

    -- 客户信息
    customid bigint COMMENT "客户ID",
    customer_name varchar COMMENT "客户名称",
    customertype_name varchar COMMENT "客户类型名称",
    customertype_group varchar COMMENT "客户类型分组",
    customer_financeclass_name varchar COMMENT "客户财务类别名称",
    is_btob tinyint default "0" COMMENT "是否云商业务(0:否, 1:是)",
    
    -- 集采信息
    jicai_type varchar COMMENT "集采类型",
    is_jicai_zhongxuan tinyint  default "0" COMMENT "是否集采中选(0:否, 1:是)",

    -- 来源相关
    comefrom varchar comment '订单来源',
    
    -- 商品信息
    goodsid bigint COMMENT "商品ID",
    goods_qty decimal(16, 6) COMMENT "商品数量",
    nianbao_type varchar COMMENT "商品年报类型",
    qixie_class varchar COMMENT "器械分类",
    qixie_brandtype varchar COMMENT "器械品牌类型",
    leibiebeizhu int COMMENT "独立单元商品类别备注",
    group_manage_type varchar COMMENT "商品集团管理类型",
    
    -- 日期和状态信息
    yewu_date datetime COMMENT "业务日期",
    create_date datetime COMMENT "生成日期",
    confirm_date datetime COMMENT "确认日期",
    use_status varchar COMMENT '使用状态: 作废/正式/临时',
    received_status varchar COMMENT "结算状态: 未收完/已收完/不收款",
    inputmanid bigint COMMENT '制单人id',
    inputman_name varchar COMMENT '制单人名称',
    
    -- 批次和价格信息
    batchid bigint COMMENT "批次ID",
    batch_notax_price decimal(18, 4) COMMENT "批次不含税价格",
    batch_notax_amount decimal(18, 4) COMMENT "批次不含税金额",
    
    -- 金额信息
    settle_amount decimal(18, 4) COMMENT "结算金额",
    notax_amount decimal(18, 4) COMMENT "不含税金额",
    cost_amount decimal(18, 4) COMMENT "成本金额",
    received_amount decimal(18, 4) COMMENT "已收款金额",
    
    -- 利润信息
    batch_gross_profit decimal(18, 4) COMMENT "批次毛利",
    batch_gross_profit_rate decimal(18, 4) COMMENT "批次毛利率",
    notax_gross_profit decimal(18, 4) COMMENT "不含税毛利",
    notax_gross_profit_rate decimal(18, 4) COMMENT "不含税毛利率"
)
-- 设置表属性
UNIQUE KEY(sasettleid,sasettledtlid) 
DISTRIBUTED BY HASH(sasettleid,sasettledtlid) 
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",  -- 副本分配策略
  "in_memory" = "false",                                 -- 是否在内存中
  "storage_format" = "V2",                               -- 存储格式
  "disable_auto_compaction" = "false"                    -- 是否禁用自动压缩
);

-- 插入基础数据：结算单信息
INSERT INTO dwd.wholesale_order_settle_dtl (
    -- 主键信息
    sasettleid,
    sasettledtlid,
    
    -- 关联信息
    salesid,
    salesdtlid,
    dw_updatetime,
    
    -- 组织信息
    entryid,
    entry_name,
    province_name,
    city_name,
    area_name,
    caiwu_level1,
    caiwu_level2,
    
    -- 客户信息
    customid,
    customer_name,
    customertype_name,
    customertype_group,
    customer_financeclass_name,
    is_btob,
    
    -- 集采信息
    jicai_type,
    is_jicai_zhongxuan,
    
    -- 来源相关
    comefrom,
    
    -- 商品信息
    goodsid,
    goods_qty,
    nianbao_type,
    qixie_class,
    qixie_brandtype,
    leibiebeizhu,
    group_manage_type,
    
    -- 日期和状态信息
    yewu_date,
    create_date,
    confirm_date,
    use_status,
    received_status,
    inputmanid,
    inputman_name,
    
    -- 批次和价格信息
    batchid,
    batch_notax_price,
    batch_notax_amount,
    
    -- 金额信息
    settle_amount,
    notax_amount,
    cost_amount,
    received_amount,
    
    -- 利润信息
    batch_gross_profit,
    batch_gross_profit_rate,
    notax_gross_profit,
    notax_gross_profit_rate
)
SELECT
    -- 主键信息
    a.sasettleid,                           
    b.sasettledtlid,
    
    -- 关联信息
    sales.salesid,
    doctoset.salesdtlid,
    b.dw_updatetime,
    
    -- 组织信息
    a.entryid,
    e.entry_name,
    e.province_name,
    e.city_name,
    e.area_name,
    e.caiwu_level1,
    e.caiwu_level2,
    
    -- 客户信息
    a.customid,
    c.customer_name,
    c.customertype_name,
    c.customertype_group,
    c.customer_financeclass_name,
    IFNULL(sales.is_btob, 0) AS is_btob,
    
    -- 集采信息
    sales.jicai_type,
    IFNULL(sales.is_jicai_zhongxuan, 0) AS is_jicai_zhongxuan,
    
    -- 来源相关
    sales.comefrom,
    
    -- 商品信息
    b.goodsid,
    b.goodsqty AS goods_qty,
    g.nianbao_type,
    g.qixie_class,
    g.qixie_brandtype,
    IFNULL(sales.leibiebeizhu, -1) AS leibiebeizhu,
    g.group_manage_type,
    
    -- 日期和状态信息
    sales.yewu_date,
    a.create_date,                
    a.confirm_date,                         
    a.use_status,
    CASE                                 
        WHEN IFNULL(b.recfinflag, 0) = 1 OR abs(b.total_line) <= abs(b.totalrecmoney) THEN '已收完'   
        WHEN IFNULL(b.recfinflag, 0) = 0 THEN '未收完'
        WHEN IFNULL(b.recfinflag, 0) = 2 THEN '不收款'
        ELSE ''
    END AS received_status,
    a.inputmanid,
    a.inputman_name,
    
    -- 批次和价格信息
    sales.batchid,
    batch.notax_price AS batch_notax_price,
    IFNULL(b.goodsqty, 0) * IFNULL(batch.notax_price, 0) AS batch_notax_amount,
    
    -- 金额信息
    b.total_line AS settle_amount,
    b.notaxmoney AS notax_amount,
    IFNULL(b.costingmoney, 0) AS cost_amount,
    b.totalrecmoney AS received_amount,
    
    -- 利润信息
    b.notaxmoney - IFNULL(b.goodsqty, 0) * IFNULL(batch.notax_price, 0) AS batch_gross_profit,
    CASE
        WHEN IFNULL(b.notaxmoney, 0) = 0 THEN CAST(0 AS decimal(18,4))
        ELSE ROUND((b.notaxmoney - IFNULL(b.goodsqty, 0) * IFNULL(batch.notax_price, 0)) / b.notaxmoney, 4) 
    END AS batch_gross_profit_rate,
    b.notaxmoney - IFNULL(b.costingmoney, 0) AS notax_gross_profit,
    CASE
        WHEN b.notaxmoney IS NULL OR b.notaxmoney = 0 THEN CAST(0 AS decimal(18,4))
        ELSE ROUND((b.notaxmoney - IFNULL(b.costingmoney, 0)) / b.notaxmoney, 4) 
    END AS notax_gross_profit_rate
FROM 
    dwd.wholesale_order_settle_doc a        -- 批发订单结算单表
INNER JOIN 
    ods_erp.bms_sa_settle_dtl b             -- ERP结算单明细表
    ON a.sasettleid = b.sasettleid
LEFT JOIN
    ods_erp.bms_sa_doctoset doctoset        -- 销售单与结算单关联表
    ON b.sasettledtlid = doctoset.sasettledtlid
LEFT JOIN
    dwd.wholesale_order_sales_dtl sales      -- 销售单明细表
    ON doctoset.salesdtlid = sales.salesdtlid
LEFT JOIN
    dim.customer c                           -- 客户维度表
    ON a.customid = c.customid 
    AND a.create_date BETWEEN c.dw_starttime AND c.dw_endtime
LEFT JOIN
    dim.entry e                              -- 独立单元维度表
    ON a.entryid = e.entryid 
    AND a.create_date BETWEEN e.dw_starttime AND e.dw_endtime
LEFT JOIN
    dim.goods g                              -- 商品维度表
    ON b.goodsid = g.goodsid 
    AND a.create_date BETWEEN g.dw_starttime AND g.dw_endtime
LEFT JOIN
    dim.batch batch                          -- 批次维度表
    ON sales.batchid = batch.batchid 
    AND a.create_date BETWEEN batch.dw_starttime AND batch.dw_endtime
WHERE 
    b.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_settle_dtl_salesid ON dwd.wholesale_order_settle_dtl (salesid);
CREATE INDEX IF NOT EXISTS idx_settle_dtl_salesdtlid ON dwd.wholesale_order_settle_dtl (salesdtlid);
CREATE INDEX IF NOT EXISTS idx_settle_dtl_entryid ON dwd.wholesale_order_settle_dtl (entryid);
CREATE INDEX IF NOT EXISTS idx_settle_dtl_customid ON dwd.wholesale_order_settle_dtl (customid);
CREATE INDEX IF NOT EXISTS idx_settle_dtl_goodsid ON dwd.wholesale_order_settle_dtl (goodsid);
CREATE INDEX IF NOT EXISTS idx_settle_dtl_batchid ON dwd.wholesale_order_settle_dtl (batchid);