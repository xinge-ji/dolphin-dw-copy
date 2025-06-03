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
WITH sales_doc_summary AS (
    -- 直接从 dwd.wholesale_sales_receivable_dtl 按销售单汇总
    SELECT 
        salesid,
        MIN(create_date) as create_date,
        MIN(yewu_date) as yewu_date,
        MAX(sale_mode) as sale_mode,
        SUM(IFNULL(sales_amount, 0)) as total_sales_amount,
        SUM(IFNULL(settle_amount, 0)) as total_settle_amount,
        MAX(settle_time) as max_settle_time,
        SUM(IFNULL(received_amount, 0)) as total_received_amount,
        MAX(received_time) as max_received_time,
        -- 判断销售单是否完全结算：所有明细都已结算
        CASE WHEN MIN(is_settled) = 1 THEN 1 ELSE 0 END as is_fully_settled,
        -- 判断销售单是否完全还款：所有明细都已还款
        CASE WHEN MIN(is_received) = 1 THEN 1 ELSE 0 END as is_fully_received
    FROM dwd.wholesale_sales_receivable_dtl
    GROUP BY salesid
)
select
    salesid,
    create_date,
    yewu_date,
    sale_mode,
    total_sales_amount as sales_amount,
    total_settle_amount as settle_amount,
    CASE WHEN is_fully_settled = 1 THEN max_settle_time ELSE NULL END as settle_time,
    is_fully_settled as is_settled,
    total_received_amount as received_amount,
    CASE WHEN is_fully_received = 1 THEN max_received_time ELSE NULL END as received_time,
    is_fully_received as is_received
from sales_doc_summary;

