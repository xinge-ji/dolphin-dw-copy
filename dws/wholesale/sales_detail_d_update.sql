DELETE FROM dws.wholesale_sales_detail_d
WHERE stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

INSERT INTO dws.wholesale_sales_detail_d (
    stat_date,
    entryid,
    customid,
    salerid,
    inputmanid,
    nianbao_type,
    jicai_type,
    order_source,
    entry_name,
    province_name,
    customer_name,
    customertype_name,
    customertype_group,
    saler_name,
    inputman_name,
    sales_amount,
    sales_gross_profit,
    order_count,
    order_item_count,
    ecommerce_sales_amount,
    non_ecommerce_sales_amount,
    ecommerce_order_count,
    non_ecommerce_order_count,
    return_amount,
    return_order_count
)
SELECT
    -- 颗粒度
    DATE(wos.create_date) AS stat_date,
    wos.entryid,
    wos.customid,
    wos.salerid AS salesman_id,
    wos.inputmanid AS recorder_id,
    wos.nianbao_type,
    wos.jicai_type,
    wos.comefrom AS order_source,
    
    -- 维度
    wos.entry_name,
    wos.province_name,
    wos.customer_name,
    wos.customertype_name,
    wos.customertype_group,
    wos.saler_name,
    wos.inputman_name,
    
    -- 销售指标
    SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_amount ELSE 0 END) AS sales_amount,
    SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_gross_profit ELSE 0 END) AS sales_gross_profit,
    COUNT(DISTINCT CASE WHEN wos.sale_type != '销退' THEN wos.salesid END) AS order_count,
    COUNT(CASE WHEN wos.sale_type != '销退' THEN wos.salesdtlid END) AS order_item_count,
    
    -- 电商相关指标
    SUM(CASE 
        WHEN wos.sale_type != '销退' AND wos.is_dianshang = 1 THEN wos.sales_amount 
        ELSE 0 
    END) AS ecommerce_sales_amount,
    
    SUM(CASE 
        WHEN wos.sale_type != '销退' AND wos.is_dianshang = 0 THEN wos.sales_amount 
        ELSE 0 
    END) AS non_ecommerce_sales_amount,
    
    COUNT(DISTINCT CASE 
        WHEN wos.sale_type != '销退' AND wos.is_dianshang = 1 THEN wos.salesid 
    END) AS ecommerce_order_count,
    
    COUNT(DISTINCT CASE 
        WHEN wos.sale_type != '销退' AND wos.is_dianshang = 0 THEN wos.salesid 
    END) AS non_ecommerce_order_count,
    
    -- 销退相关指标
    SUM(CASE 
        WHEN wos.sale_type = '销退' THEN wos.sales_amount 
        ELSE 0 
    END) AS return_amount,
    
    COUNT(DISTINCT CASE 
        WHEN wos.sale_type = '销退' THEN wos.salesid 
    END) AS return_order_count
FROM
    dwd.wholesale_order_sales_dtl wos
WHERE
    wos.use_status = '正式' 
    AND DATE(wos.create_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    AND DATE(wos.create_date) < CURRENT_DATE()
GROUP BY
    DATE(wos.create_date),
    wos.entryid,
    wos.customid,
    wos.salerid,
    wos.inputmanid,
    wos.nianbao_type,
    wos.jicai_type,
    wos.comefrom,
    wos.entry_name,
    wos.province_name,
    wos.customer_name,
    wos.customertype_name,
    wos.customertype_group,
    wos.saler_name,
    wos.inputman_name;