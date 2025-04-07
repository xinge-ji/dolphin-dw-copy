INSERT INTO dwd.wholesale_order_repay_dtl (sarecid,sarecdtlid, __DORIS_DELETE_SIGN__)
SELECT a.sarecid, a.sarecdtlid, 1
FROM ods_erp.bms_sa_rec_dtl AS a
JOIN dwd.wholesale_order_repay_dtl AS b
ON a.sarecid = b.sarecid AND a.sarecdtlid = b.sarecdtlid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

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
    b.is_active = 1
  AND b.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.wholesale_order_repay_dtl);
