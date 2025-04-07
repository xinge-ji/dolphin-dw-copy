DELETE FROM dws.wholesale_customer_sales_d
WHERE stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

INSERT INTO dws.wholesale_customer_sales_d (
    stat_date,
    entryid,
    customid,
    entry_name,
    province_name,
    customer_name,
    customertype_name,
    customertype_group,
    sales_amount,
    sales_gross_profit,
    order_count,
    order_item_count,
    adjustment_amount,
    adjustment_order_count
)
SELECT
    -- 颗粒度
    DATE(wos.create_date) AS stat_date,
    wos.entryid,
    wos.customid,
    
    -- 维度
    wos.entry_name,
    wos.province_name,
    wos.customer_name,
    wos.customertype_name,
    wos.customertype_group,
    
    -- 销售指标
    SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_amount ELSE 0 END) AS sales_amount,
    SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_gross_profit ELSE 0 END) AS sales_gross_profit,
    COUNT(DISTINCT CASE WHEN wos.sale_type != '销退' THEN wos.salesid END) AS order_count,
    COUNT(CASE WHEN wos.sale_type != '销退' THEN wos.salesdtlid END) AS order_item_count,
    
    -- 冲差相关指标
    SUM(CASE 
        WHEN wos.sale_type = '销退' AND wos.comefrom = '手工录入' THEN wos.sales_amount 
        ELSE 0 
    END) AS adjustment_amount,
    
    COUNT(DISTINCT CASE 
        WHEN wos.sale_type = '销退' AND wos.comefrom = '手工录入' THEN wos.salesid 
    END) AS adjustment_order_count
FROM
    dwd.wholesale_order_sales_dtl wos
WHERE
    wos.use_status = '正式' 
    AND date(wos.create_date) >= CURRENT_DATE() - INTERVAL 60 DAY
    AND date(wos.create_date) < CURRENT_DATE()
GROUP BY
    DATE(wos.create_date),
    wos.entryid,
    wos.customid,
    wos.entry_name,
    wos.province_name,
    wos.customer_name,
    wos.customertype_name,
    wos.customertype_group;