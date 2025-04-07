DELETE FROM dws.wholesale_settle_goods_d
WHERE stat_date > CURRENT_DATE() - INTERVAL 60 DAY;

-- 插入数据：按日期、商品、独立单元维度聚合
INSERT INTO dws.wholesale_settle_goods_d (
    stat_date,
    goodsid,
    entryid,
    entry_name,
    province_name,
    city_name,
    area_name,
    caiwu_level1,
    caiwu_level2,
    customid,
    customer_name,
    customertype_name,
    customertype_group,
    customer_financeclass_name,
    is_jicai_zhongxuan,
    nianbao_type,
    qixie_class,
    qixie_brandtype,
    leibiebeizhu,
    group_manage_type,
    goods_qty,
    settle_amount,
    notax_amount,
    cost_amount,
    batch_gross_profit,
    notax_gross_profit,
    gross_profit_rate
)
SELECT
    DATE(confirm_date) AS stat_date,
    goodsid,
    entryid,
    entry_name,
    province_name,
    city_name,
    area_name,
    caiwu_level1,
    caiwu_level2,
    customid,
    customer_name,
    customertype_name,
    customertype_group,
    customer_financeclass_name,
    is_jicai_zhongxuan,
    IFNULL(nianbao_type, 'UNKNOWN') as nianbao_type,
    IFNULL(qixie_class, 'UNKNOWN') as qixie_class,
    IFNULL(qixie_brandtype, 'UNKNOWN') as qixie_brandtype,
    IFNULL(leibiebeizhu, -1) as leibiebeizhu,
    group_manage_type,
    SUM(goods_qty) AS goods_qty,
    SUM(settle_amount) AS settle_amount,
    SUM(notax_amount) AS notax_amount,
    SUM(cost_amount) AS cost_amount,
    SUM(batch_gross_profit) AS batch_gross_profit,
    SUM(notax_gross_profit) AS notax_gross_profit,
    CASE 
        WHEN SUM(notax_amount) = 0 THEN CAST(0 as decimal(18,4))
        ELSE ROUND(SUM(notax_gross_profit) / SUM(notax_amount), 4)
    END AS gross_profit_rate
FROM 
    dwd.wholesale_order_settle_dtl
WHERE 
    use_status = '正式'
    AND date(confirm_date) >= CURRENT_DATE() - INTERVAL 60 DAY 
    AND date(confirm_date) <= CURRENT_DATE() - INTERVAL 1 DAY 
GROUP BY 
    DATE(confirm_date),
    goodsid,
    entryid,
    entry_name,
    province_name,
    city_name,
    area_name,
    caiwu_level1,
    caiwu_level2,
    customid,
    customer_name,
    customertype_name,
    customertype_group,
    customer_financeclass_name,
    is_jicai_zhongxuan,
    nianbao_type,
    qixie_class,
    qixie_brandtype,
    leibiebeizhu,
    group_manage_type;