INSERT INTO dwd.wholesale_order_settle_dtl (sasettleid,sasettledtlid, __DORIS_DELETE_SIGN__)
SELECT a.sasettleid,a.sasettledtlid, 1
FROM ods_erp.bms_sa_settle_dtl AS a
JOIN dwd.wholesale_order_settle_doc AS b 
ON a.sasettleid = b.sasettleid AND a.sasettledtlid = b.sasettledtlid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

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
        WHEN IFNULL(b.recfinflag, 0) = 2 THEN '不收款'
        ELSE '需要收款'
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
    AND a.create_date >= c.dw_starttime AND a.create_date < c.dw_endtime
LEFT JOIN
    dim.entry e                              -- 独立单元维度表
    ON a.entryid = e.entryid 
    AND a.create_date >= e.dw_starttime AND a.create_date < e.dw_endtime
LEFT JOIN
    dim.goods g                              -- 商品维度表
    ON b.goodsid = g.goodsid 
    AND a.create_date >= g.dw_starttime AND a.create_date < g.dw_endtime
LEFT JOIN
    dim.batch batch                          -- 批次维度表
    ON sales.batchid = batch.batchid 
    AND a.create_date >= batch.dw_starttime AND a.create_date < batch.dw_endtime
WHERE 
    b.is_active = 1
    AND b.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.wholesale_order_settle_dtl);