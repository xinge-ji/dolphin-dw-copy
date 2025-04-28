DELETE FROM dws.wholesale_sales_goods_d
WHERE stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

INSERT INTO dws.wholesale_sales_goods_d (
    stat_date,
    goodsid,
    entryid,
    customid,
    entry_name,
    province_name,
    city_name,
    area_name,
    customer_name,
    customertype_task,
    goods_name,
    sales_amount,
    return_amount
)
SELECT
    DATE(wos.create_date) AS stat_date,
    wos.goodsid,
    wos.entryid,
    wos.customid,
    wos.entry_name,
    wos.province_name,
    wos.city_name,
    wos.area_name,
    wos.customer_name,
    c.customertype_task,
    wos.goods_name,
    SUM(CASE WHEN wos.sale_type != '销退' THEN wos.sales_amount ELSE 0 END) AS sales_amount,
    SUM(CASE WHEN wos.sale_type = '销退' THEN wos.sales_amount ELSE 0 END) AS return_amount
FROM
    dwd.wholesale_order_sales_dtl wos
LEFT JOIN
    dim.customer c ON wos.customid = c.customid 
LEFT JOIN
    dim.entry e ON wos.entryid = e.entryid
WHERE
    wos.use_status = '正式'
    AND DATE(wos.create_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    AND DATE(wos.create_date) < CURRENT_DATE()
GROUP BY
    DATE(wos.create_date),
    wos.goodsid,
    wos.entryid,
    wos.customid,
    wos.entry_name,
    wos.province_name,
    wos.city_name,
    wos.area_name,
    wos.customer_name,
    c.customertype_task,
    wos.goods_name;