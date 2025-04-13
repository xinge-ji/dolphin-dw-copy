DROP TABLE IF EXISTS dws.wholesale_sales_goods_d;

-- 创建批发订单结算商品日汇总表
CREATE TABLE dws.wholesale_sales_goods_d (
    -- 维度信息
    stat_date DATE COMMENT "业务日期",
    goodsid bigint COMMENT "商品ID",
    entryid bigint COMMENT "独立单元ID",
    customid bigint COMMENT "客户ID",

    -- 组织信息
    entry_name varchar COMMENT "独立单元名称",
    province_name varchar COMMENT "独立单元省份名称",
    city_name varchar COMMENT "独立单元城市名称",
    area_name varchar COMMENT "独立单元地区名称",

    -- 客户信息
    customer_name varchar COMMENT "客户名称",
    customertype_task varchar comment '分销项目客户类型:等级机构/基层/终端',
    
    -- 商品信息
    goods_name varchar COMMENT "商品名称",
    
    -- 金额指标
    sales_amount decimal(18,4) COMMENT '销售金额',
    return_amount decimal(18,4) COMMENT '销退金额'
)
UNIQUE KEY(stat_date, goodsid, entryid, customid) 
DISTRIBUTED BY HASH(stat_date, goodsid) 
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 插入数据
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

-- 创建索引提高查询性能
CREATE INDEX IF NOT EXISTS idx_stat_date ON dws.wholesale_sales_goods_d (stat_date);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dws.wholesale_sales_goods_d (goodsid);
CREATE INDEX IF NOT EXISTS idx_entryid ON dws.wholesale_sales_goods_d (entryid);
CREATE INDEX IF NOT EXISTS idx_customid ON dws.wholesale_sales_goods_d (customid);
CREATE INDEX IF NOT EXISTS idx_customertype_task ON dws.wholesale_sales_goods_d (customertype_task);