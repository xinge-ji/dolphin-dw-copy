drop table if exists dwd.wholesale_sales_receivable_doc;
create table dwd.wholesale_sales_receivable_doc (
    -- 颗粒度
    salesid varchar(255) COMMENT '销售单ID',

    -- 销售相关
    create_date datetime COMMENT '销售单创建日期',
    yewu_date datetime COMMENT '业务日期',
    sale_mode varchar(50) COMMENT '销售模式',
    sales_amount decimal(18,2) COMMENT '销售金额',

    -- 结算相关
    settle_amount decimal(18,2) COMMENT '订单结算金额',
    settle_time datetime COMMENT '订单完全结算时间（结算完成时的最大确认日期）',
    is_settled int COMMENT '订单结算状态（1=已完全结算，0=未完全结算）',

    -- 还款相关
    received_amount decimal(18,4) COMMENT '订单付款金额',
    received_time datetime COMMENT '订单完全结算时间（结算完成时的最大确认日期）',
    is_received int COMMENT '订单还款状态（1=已完全还款，0=未完全还款）'
)
UNIQUE KEY(salesid) 
DISTRIBUTED BY HASH(salesid) 
PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",  -- 副本分配策略
  "in_memory" = "false",                                 -- 是否在内存中
  "storage_format" = "V2",                               -- 存储格式
  "disable_auto_compaction" = "false"                    -- 是否禁用自动压缩
);

insert into dwd.wholesale_sales_receivable_doc (
    salesid,
    create_date,
    yewu_date,
    sale_mode,
    sales_amount,
    settle_amount,
    settle_time,
    is_settled,
    received_amount,
    received_time,
    is_received
)
WITH fully_settled_order AS (
    -- 计算销售单是否完全结算
    SELECT 
        a.salesid,
        CASE
            WHEN MIN(IFNULL(b.is_settled, 0)) = 1 THEN MAX(b.settle_time) 
            ELSE NULL
        END AS settle_time,
        SUM(b.settle_amount) AS settle_amount
    FROM dwd.wholesale_order_sales_doc a
    LEFT JOIN dwd.wholesale_sales_receivable_dtl b ON a.salesid = b.salesid
    GROUP BY a.salesid
),
fully_paid_order AS (
    -- 计算销售单是否完全还款
    SELECT 
        a.salesid,
        CASE
            WHEN MIN(IFNULL(b.is_received, 0)) = 1 THEN MAX(b.received_time) 
            ELSE NULL
        END AS received_time,
        SUM(b.received_amount) AS received_amount
    FROM dwd.wholesale_order_sales_doc a
    LEFT JOIN dwd.wholesale_sales_receivable_dtl b ON a.salesid = b.salesid
    GROUP BY a.salesid
)
SELECT
    a.salesid,
    a.create_date,
    d.yewu_date,
    a.sale_mode,
    a.sales_amount,
    IFNULL(b.settle_amount, 0) AS settle_amount,
    b.settle_time,
    CASE WHEN b.settle_time IS NOT NULL THEN 1 ELSE 0 END AS is_settled,
    IFNULL(c.received_amount, 0) AS received_amount,
    c.received_time,
    CASE WHEN c.received_time IS NOT NULL THEN 1 ELSE 0 END AS is_received
FROM dwd.wholesale_order_sales_doc a
LEFT JOIN (SELECT salesid, MIN(yewu_date) AS yewu_date FROM dwd.wholesale_order_sales_dtl GROUP BY salesid) d ON a.salesid = d.salesid
LEFT JOIN fully_settled_order b ON a.salesid = b.salesid
LEFT JOIN fully_paid_order c ON a.salesid = c.salesid;
