INSERT INTO dwd.wholesale_order_sales_dtl (salesid, salesdtlid, __DORIS_DELETE_SIGN__)
SELECT a.salesid, a.salesdtlid, 1
FROM ods_erp.bms_sa_dtl AS a
JOIN dwd.wholesale_order_sales_doc AS b 
ON a.salesid = b.salesid AND a.salesdtlid = b.salesdtlid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

-- 将基础数据和部分维度信息一次性插入
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
    settle_amount,
    received_amount,
    
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
        WHEN IFNULL(b.settleflag, 0) = 1 OR abs(b.total_line) <= abs(b.settlemoney) THEN '结算完成'
        WHEN IFNULL(b.settleflag, 0) = 0 THEN '未结算完成'
        WHEN IFNULL(b.settleflag, 0) = 2 THEN '不结算'
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
    b.settlemoney, 
    b.totalrecmoney,

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
    b.is_active = 1
    AND b.dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.wholesale_order_sales_dtl);