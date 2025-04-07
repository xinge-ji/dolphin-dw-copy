DROP TABLE IF EXISTS dwd.wholesale_order_repay_dtl;
CREATE TABLE dwd.wholesale_order_repay_dtl (
    -- 颗粒度
	sarecid bigint COMMENT '还款总单ID',
    sarecdtlid bigint COMMENT '还款明细ID',
    
    -- 基础信息
    dw_updatetime datetime COMMENT '更新时间',

    -- 关联信息
    sasettledtlid bigint COMMENT '结算单明细ID',
    sasettleid bigint COMMENT '结算单ID',
    salesdtlid bigint COMMENT '销售单明细ID',
    salesid bigint COMMENT '销售单ID',

    -- 组织维度
    entryid bigint COMMENT '独立单元ID',
    entry_name varchar COMMENT '独立单元名称',
    province_name varchar COMMENT '省份名称',
    city_name varchar COMMENT '城市名称',

    -- 客户维度
    customid bigint COMMENT '客户ID',
    customer_name varchar COMMENT '客户名称',

    -- 订单维度
    is_btob tinyint default "0" COMMENT '是否云商业务',
    jicai_type varchar COMMENT '集采类型',

    -- 商品维度
    goodsid bigint COMMENT '商品ID',
    nianbao_type varchar COMMENT '年报类型',

    -- 还款维度
    yewu_date datetime COMMENT '业务日期',
    create_date datetime COMMENT '创建日期',
    confirm_date datetime COMMENT '确认日期', 
    shoukuan_type varchar COMMENT '收款类型',
    use_status varchar COMMENT '使用状态',
    is_yibao_payment tinyint COMMENT '是否医保支付(0:否, 1:是)',
    yushoukuan_transfer_date datetime COMMENT '预收款转出日期',
    payment_amount decimal(18,4) COMMENT '付款金额',
    payment_date datetime COMMENT '付款日期'
)
UNIQUE KEY(sarecid, sarecdtlid) DISTRIBUTED BY HASH(sarecid, sarecdtlid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

INSERT INTO dwd.wholesale_order_repay_dtl (
    -- 颗粒度
    sarecid,
    sarecdtlid,
    
    -- 基础信息
    dw_updatetime,
    
    -- 关联信息
    sasettledtlid,
    sasettleid,
    salesdtlid,
    salesid,
    
    -- 组织维度
    entryid,
    entry_name,
    province_name,
    city_name,
    
    -- 客户维度
    customid,
    customer_name,
    
    -- 订单维度
    is_btob,
    jicai_type,
    
    -- 商品维度
    goodsid,
    nianbao_type,
    
    -- 还款维度
    yewu_date,
    create_date,
    confirm_date,
    shoukuan_type,
    use_status,
    is_yibao_payment,
    yushoukuan_transfer_date,
    payment_amount,
    payment_date
)
SELECT
    -- 颗粒度
    a.sarecid,                                  -- 还款总单ID
    b.sarecdtlid,                               -- 还款明细ID
    
    -- 基础信息
    b.dw_updatetime,                            -- 更新时间
    
    -- 关联信息
    settorec.sasettledtlid,                     -- 结算单明细ID
    settle.sasettleid,                          -- 结算单ID
    settle.salesdtlid,                          -- 销售单明细ID
    settle.salesid,                             -- 销售单ID
    
    -- 组织维度
    settle.entryid,                             -- 独立单元ID
    settle.entry_name,                          -- 独立单元名称
    settle.province_name,                       -- 省份名称
    settle.city_name,                           -- 城市名称
    
    -- 客户维度
    settle.customid,                            -- 客户ID
    settle.customer_name,                       -- 客户名称
    
    -- 订单维度
    IFNULL(settle.is_btob, 0),                  -- 是否B2B业务
    settle.jicai_type,                          -- 集采类型
    
    -- 商品维度
    settle.goodsid,                             -- 商品ID
    COALESCE(settle.nianbao_type,               -- 优先使用结算单的年报类型
             goods.nianbao_type),               -- 如果为空则使用商品维度表的年报类型
    
    -- 还款维度
    settle.yewu_date,                           -- 业务日期
    a.create_date,                              -- 创建日期
    a.confirm_date,                             -- 确认日期
    a.shoukuan_type,                            -- 收款类型
    a.use_status,                               -- 使用状态
    a.is_yibao_payment,                          -- 是否医保支付
    b.cdate,                                    -- 预收款转出日期
    b.total_line,                               -- 付款金额
    CASE
        WHEN a.shoukuan_type='预收款' THEN b.cdate
        ELSE a.confirm_date
    END AS payment_date                         -- 付款日期
FROM 
    dwd.wholesale_order_repay_doc a             -- 还款单主表
INNER JOIN 
    ods_erp.bms_sa_rec_dtl b                    -- 还款单明细表
    ON a.sarecid = b.sarecid
LEFT JOIN 
    ods_erp.bms_sa_settorec settorec            -- 结算单与还款单关联表
    ON b.sarecdtlid = settorec.sarecdtlid
LEFT JOIN 
    dwd.wholesale_order_settle_dtl settle       -- 结算单明细表
    ON settorec.sasettledtlid = settle.sasettledtlid
LEFT JOIN 
    dim.goods goods                             -- 商品维度表
    ON settle.goodsid = goods.goodsid 
    AND a.create_date >= goods.dw_starttime AND a.create_date < goods.dw_endtime
WHERE 
    b.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_repay_dtl_sarecid ON dwd.wholesale_order_repay_dtl (sarecid);
CREATE INDEX IF NOT EXISTS idx_repay_dtl_sarecdtlid ON dwd.wholesale_order_repay_dtl (sarecdtlid);
CREATE INDEX IF NOT EXISTS idx_repay_dtl_sasettledtlid ON dwd.wholesale_order_repay_dtl (sasettledtlid);
CREATE INDEX IF NOT EXISTS idx_repay_dtl_sasettleid ON dwd.wholesale_order_repay_dtl (sasettleid);
CREATE INDEX IF NOT EXISTS idx_repay_dtl_salesid ON dwd.wholesale_order_repay_dtl (salesid);
CREATE INDEX IF NOT EXISTS idx_repay_dtl_salesdtlid ON dwd.wholesale_order_repay_dtl (salesdtlid);
CREATE INDEX IF NOT EXISTS idx_repay_dtl_entryid ON dwd.wholesale_order_repay_dtl (entryid);
CREATE INDEX IF NOT EXISTS idx_repay_dtl_customid ON dwd.wholesale_order_repay_dtl (customid);
CREATE INDEX IF NOT EXISTS idx_repay_dtl_goodsid ON dwd.wholesale_order_repay_dtl (goodsid);