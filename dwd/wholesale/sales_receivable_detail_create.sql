drop table if exists dwd.wholesale_sales_receivable_detail;
create table dwd.wholesale_sales_receivable_detail (
    -- 颗粒度
    salesid varchar(255) COMMENT '销售单ID',
    salesdtlid varchar(255) COMMENT '销售单明细ID',

    -- 组织信息
    entryid bigint COMMENT "独立单元ID",
    entry_name varchar COMMENT "独立单元名称",
    province_name varchar COMMENT "独立单元省份名称",
    city_name varchar COMMENT "独立单元城市名称",
    area_name varchar COMMENT "独立单元地区名称",
    caiwu_level1 varchar COMMENT "集团模块一级",
    caiwu_level2 varchar COMMENT "集团模块二级",

    -- 客户信息
    customid bigint COMMENT "客户ID",
    customer_name varchar COMMENT "客户名称",
    customertype_name varchar COMMENT "客户类型名称",
    customertype_group varchar COMMENT "客户类型分组",
    customer_financeclass_name varchar COMMENT "客户财务类别名称",
    is_btob tinyint default "0" COMMENT "是否云商业务(0:否, 1:是)",

    -- 集采信息
    jicai_type varchar COMMENT "集采类型",
    is_jicai_zhongxuan tinyint  default "0" COMMENT "是否集采中选(0:否, 1:是)",

    -- 来源相关
    comefrom varchar comment '订单来源',

    -- 销售相关
    create_date datetime COMMENT '销售单创建日期',
    yewu_date datetime COMMENT '业务日期',
    sale_mode varchar(50) COMMENT '销售模式',
    item_sales_settle_status varchar(50) COMMENT '销售单结算状态',
    item_sales_amount decimal(18,2) COMMENT '销售金额',

    -- 结算相关
    order_settle_amount decimal(18,2) COMMENT '订单结算金额',
    order_settle_time datetime COMMENT '订单完全结算时间（所有明细都结算完成时的最大确认日期）',
    order_settle_status int COMMENT '订单结算状态（1=已完全结算，0=未完全结算）',
    order_item_settle_amount decimal(18,2) COMMENT '订单明细结算金额',
    order_item_settle_time datetime COMMENT '订单明细完全结算时间（明细结算完成时的最大确认日期）',
    order_item_settle_status int COMMENT '订单明细结算状态（1=已完全结算，0=未完全结算）',

    -- 还款相关
    order_payment_amount decimal(18,4) COMMENT '订单付款金额',
    order_payment_time datetime COMMENT '订单完全结算时间（所有明细都结算完成时的最大确认日期）',
    order_payment_status int COMMENT '订单结算状态（1=已完全结算，0=未完全结算）',
    order_item_payment_amount decimal(18,4) COMMENT '订单明细付款金额',
    order_item_payment_time datetime COMMENT '订单明细完全结算时间（明细结算完成时的最大确认日期）',
    order_item_payment_status int COMMENT '订单明细还款状态（1=已完全还款，0=未完全还款）',
)
UNIQUE KEY(salesid,salesdtlid) 
DISTRIBUTED BY HASH(salesid) 
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",  -- 副本分配策略
  "in_memory" = "false",                                 -- 是否在内存中
  "storage_format" = "V2",                               -- 存储格式
  "disable_auto_compaction" = "false"                    -- 是否禁用自动压缩
);

-- 插入全部数据的版本
insert into dwd.wholesale_sales_receiable_detail(
    salesid,
    salesdtlid,
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
    is_btob,
    jicai_type,
    is_jicai_zhongxuan,
    comefrom,
    create_date,
    yewu_date,
    sale_mode,
    item_sales_settle_status,
    item_sales_amount,
    order_settle_amount,
    order_settle_time,
    order_settle_status,
    order_item_settle_amount,
    order_item_settle_time,
    order_item_settle_status,
    order_payment_amount,
    order_payment_time,
    order_payment_status,
    order_item_payment_amount,
    order_item_payment_time,
    order_item_payment_status
)
WITH fully_settled_order AS (
    SELECT 
        a.salesid,
        MIN(CASE WHEN a.settle_status = '结算完成' OR b.settle_status in ('已收完', '不收款') THEN 1 ELSE 0 END) AS all_settled,
        MAX(b.confirm_date) AS max_confirm_date
    FROM dwd.wholesale_order_sales_dtl a
    LEFT JOIN dwd.wholesale_order_settle_dtl b ON a.salesdtlid = b.salesdtlid
    WHERE a.salesid IS NOT NULL
    GROUP BY a.salesid
    HAVING MIN(CASE WHEN a.settle_status = '结算完成' OR b.settle_status in ('已收完', '不收款') THEN 1 ELSE 0 END) = 1
),
fully_settled_order_item AS (
    SELECT 
        a.salesdtlid,
        MIN(CASE WHEN a.settle_status = '结算完成' OR b.settle_status in ('已收完', '不收款') THEN 1 ELSE 0 END) AS all_settled,
        MAX(b.confirm_date) AS max_confirm_date
    FROM dwd.wholesale_order_sales_dtl a
    LEFT JOIN dwd.wholesale_order_settle_dtl b ON a.salesdtlid = b.salesdtlid
    WHERE a.salesdtlid IS NOT NULL
    GROUP BY a.salesdtlid
    HAVING MIN(CASE WHEN a.settle_status = '结算完成' OR b.settle_status in ('已收完', '不收款') THEN 1 ELSE 0 END) = 1
),
fully_paid_settle_dtl AS (
    -- 先计算每个结算单明细的还款总额是否足够结清
    SELECT 
        s.sasettledtlid,
        s.salesid,
        s.salesdtlid,
        MAX(r.payment_date) AS max_payment_date,
        CASE 
            WHEN SUM(IFNULL(r.payment_amount, 0)) >= s.sales_amount THEN 1
            ELSE 0
        END AS is_fully_paid
    FROM dwd.wholesale_order_settle_dtl s
    LEFT JOIN dwd.wholesale_order_repay_dtl r ON s.sasettledtlid = r.sasettledtlid AND r.use_status = '正式'
    WHERE s.use_status = '正式' and (r.settle_status != '不收款' OR r.settle_status IS NULL)
    GROUP BY s.sasettledtlid, s.salesid, s.salesdtlid, s.sales_amount
),
fully_paid_order AS (
    -- 基于结算单明细的还款状态，计算整个销售单是否完全还清
    SELECT 
        salesid,
        MIN(is_fully_paid) AS all_paid,  -- 只有当所有结算单明细都已还清时，销售单才算完全还清
        MAX(max_payment_date) AS max_payment_date
    FROM fully_paid_settle_dtl
    WHERE salesid IS NOT NULL
    GROUP BY salesid
    HAVING MIN(is_fully_paid) = 1  -- 所有结算单明细都已还清
),
fully_paid_order_item AS (
    -- 基于结算单明细的还款状态，计算销售单明细是否完全还清
    SELECT 
        salesdtlid,
        MIN(is_fully_paid) AS all_paid,  -- 只有当所有关联的结算单明细都已还清时，销售单明细才算完全还清
        MAX(max_payment_date) AS max_payment_date
    FROM fully_paid_settle_dtl
    WHERE salesdtlid IS NOT NULL
    GROUP BY salesdtlid
    HAVING MIN(is_fully_paid) = 1  -- 所有关联的结算单明细都已还清
)
SELECT 
    a.salesid, 
    a.salesdtlid, 
    COALESCE(a.entryid, b.entryid) AS entryid, 
    COALESCE(a.entry_name, b.entry_name) AS entry_name,
    COALESCE(a.province_name, b.province_name) AS province_name,
    COALESCE(a.city_name, b.city_name) AS city_name,
    COALESCE(a.area_name, b.area_name) AS area_name,
    COALESCE(a.caiwu_level1, b.caiwu_level1) AS caiwu_level1,
    COALESCE(a.caiwu_level2, b.caiwu_level2) AS caiwu_level2,
    COALESCE(a.customid, b.customid) AS customid, 
    COALESCE(a.customer_name, b.customer_name) AS customer_name,
    COALESCE(a.customertype_name, b.customertype_name) AS customertype_name,
    COALESCE(a.customertype_group, b.customertype_group) AS customertype_group,
    COALESCE(a.customer_financeclass_name, b.customer_financeclass_name) AS customer_financeclass_name,
    COALESCE(a.is_btob, b.is_btob, 0) AS is_btob,
    COALESCE(a.jicai_type, b.jicai_type) AS jicai_type,
    COALESCE(a.is_jicai_zhongxuan, b.is_jicai_zhongxuan, 0) AS is_jicai_zhongxuan,
    COALESCE(a.comefrom, b.comefrom) AS comefrom,
    a.create_date, 
    a.yewu_date,
    a.sale_mode, 
    a.settle_status AS item_sales_settle_status, 
    a.sales_amount AS item_sales_amount,
    -- 订单级别结算数据
    (SELECT SUM(IFNULL(s.sales_amount, 0)) 
     FROM dwd.wholesale_order_settle_dtl s 
     WHERE s.salesid = a.salesid AND s.use_status = '正式') AS order_settle_amount,
    CASE 
        WHEN fs.salesid IS NOT NULL THEN fs.max_confirm_date
        ELSE NULL
    END AS order_settle_time,
    CASE 
        WHEN fs.salesid IS NOT NULL THEN 1
        ELSE 0
    END AS order_settle_status,
    -- 订单明细级别结算数据
    (SELECT SUM(IFNULL(s.sales_amount, 0)) 
     FROM dwd.wholesale_order_settle_dtl s 
     WHERE s.salesdtlid = a.salesdtlid AND s.use_status = '正式') AS order_item_settle_amount,
    CASE
        WHEN fsi.salesdtlid IS NOT NULL THEN fsi.max_confirm_date
        ELSE NULL
    END AS order_item_settle_time,
    CASE
        WHEN fsi.salesdtlid IS NOT NULL THEN 1
        ELSE 0
    END AS order_item_settle_status,
    -- 订单级别还款数据
    (SELECT SUM(IFNULL(r.payment_amount, 0)) 
     FROM dwd.wholesale_order_settle_dtl s
     LEFT JOIN dwd.wholesale_order_repay_dtl r ON s.sasettledtlid = r.sasettledtlid AND r.use_status = '正式'
     WHERE s.salesid = a.salesid AND s.use_status = '正式') AS order_payment_amount,
    CASE 
        WHEN fpo.salesid IS NOT NULL THEN fpo.max_payment_date
        ELSE NULL
    END AS order_payment_time,
    CASE 
        WHEN fpo.salesid IS NOT NULL THEN 1
        ELSE 0
    END AS order_payment_status,
    -- 订单明细级别还款数据
    (SELECT SUM(IFNULL(r.payment_amount, 0)) 
     FROM dwd.wholesale_order_settle_dtl s
     LEFT JOIN dwd.wholesale_order_repay_dtl r ON s.sasettledtlid = r.sasettledtlid AND r.use_status = '正式'
     WHERE s.salesdtlid = a.salesdtlid AND s.use_status = '正式') AS order_item_payment_amount,
    CASE
        WHEN fpoi.salesdtlid IS NOT NULL THEN fpoi.max_payment_date
        ELSE NULL
    END AS order_item_payment_time,
    CASE
        WHEN fpoi.salesdtlid IS NOT NULL THEN 1
        ELSE 0
    END AS order_item_payment_status
FROM dwd.wholesale_order_sales_dtl a
LEFT JOIN dwd.wholesale_order_settle_dtl b ON a.salesdtlid = b.salesdtlid
LEFT JOIN fully_settled_order fs ON a.salesid = fs.salesid
LEFT JOIN fully_settled_order_item fsi ON a.salesdtlid = fsi.salesdtlid
LEFT JOIN fully_paid_order fpo ON a.salesid = fpo.salesid
LEFT JOIN fully_paid_order_item fpoi ON a.salesdtlid = fpoi.salesdtlid
WHERE a.use_status = '正式'
AND (b.use_status = '正式' OR b.use_status IS NULL);


CREATE INDEX IF NOT EXISTS idx_salesid ON dwd.wholesale_sales_receivable_detail (salesid);
CREATE INDEX IF NOT EXISTS idx_salesdtlid ON dwd.wholesale_sales_receivable_detail (salesdtlid);
CREATE INDEX IF NOT EXISTS idx_entryid ON dwd.wholesale_sales_receivable_detail (entryid);
CREATE INDEX IF NOT EXISTS idx_customid ON dwd.wholesale_sales_receivable_detail (customid);
CREATE INDEX IF NOT EXISTS idx_create_date ON dwd.wholesale_sales_receivable_detail (create_date);
CREATE INDEX IF NOT EXISTS idx_order_settle_time ON dwd.wholesale_sales_receivable_detail (order_settle_time);
CREATE INDEX IF NOT EXISTS idx_order_settle_status ON dwd.wholesale_sales_receivable_detail (order_settle_status);
CREATE INDEX IF NOT EXISTS idx_order_payment_time ON dwd.wholesale_sales_receivable_detail (order_payment_time);
CREATE INDEX IF NOT EXISTS idx_order_payment_status ON dwd.wholesale_sales_receivable_detail (order_payment_status);

