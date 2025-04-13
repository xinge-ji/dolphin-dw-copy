DROP TABLE IF EXISTS ads.wholesale_task_goods_q;
CREATE TABLE ads.wholesale_task_goods_q (
    -- 颗粒度
    stat_yearquarter DATE COMMENT "业务年季度",
    entryid bigint COMMENT '独立单元ID',
    docid bigint COMMENT '项目ID',
    goodsid bigint COMMENT '商品ID',
    
    -- 基础信息
    task_name varchar(255) COMMENT '项目名称',
    entry_name varchar(255) COMMENT '独立单元名称',
    goods_name varchar(255) COMMENT '商品名称',

    -- 维度
    area_name varchar(255) COMMENT '区域名称',
    
    -- 指标
    sales_amount decimal(20,2) COMMENT '销售额',
    new_customer_sales_amount decimal(20,2) COMMENT '准入客户销售额',
    new_customer_count bigint COMMENT '准入客户数:在项目周期内没有销售记录，在本季度存在销售记录',
    retention_customer_count bigint COMMENT '留存客户数:在项目周期内没有销售记录，在本季度存在多条销售记录'
) UNIQUE KEY (stat_yearmonth, entryid, docid, goodsid) DISTRIBUTED BY HASH (docid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
);