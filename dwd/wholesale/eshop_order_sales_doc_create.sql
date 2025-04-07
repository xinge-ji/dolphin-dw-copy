DROP TABLE IF EXISTS dwd.eshop_order_sales_doc;
CREATE TABLE dwd.eshop_order_sales_doc (
	order_id bigint COMMENT '订单ID',
    create_time datetime COMMENT '创建时间',
    update_time datetime COMMENT '更新时间',
    org_id bigint COMMENT '独立单元ID对应ID',
    entryid bigint COMMENT '独立单元ID',
    buyers_id bigint COMMENT '客户ID对应云商ID',
    customid bigint COMMENT '客户ID',
    process_stat_code varchar COMMENT '处理状态代码',
    process_status varchar COMMENT '处理状态描述',
    is_salesman_order tinyint COMMENT '是否销售员订单(1:是,0:否)',
    salesman_id bigint COMMENT '销售员ID',
    salesman_name varchar COMMENT '销售员姓名',
    sales_amount decimal(18,4) COMMENT '销售金额'
)
UNIQUE KEY(order_id) DISTRIBUTED BY HASH(order_id) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

INSERT INTO dwd.eshop_order_sales_doc (
    order_id,
    create_time,
    update_time,
    org_id,
    entryid,
    buyers_id,
    customid,
    process_stat_code,
    process_status,
    is_salesman_order,
    salesman_id,
    salesman_name,
    sales_amount
) 
SELECT
    t1.order_id,
    t1.create_time,
    t1.update_time,
    t1.org_id,
    t2.entryid,
    t1.buyers_id,
    t3.customid,
    t1.process_stat_code,
    CASE
        WHEN t1.process_stat_code = 'WAIT_APPROVE' THEN '待审核'
        WHEN t1.process_stat_code = 'WAIT_SEND' THEN '待发货'
        WHEN t1.process_stat_code = 'SENDING' THEN '发货中'
        WHEN t1.process_stat_code = 'SEND' THEN '已发货'
        WHEN t1.process_stat_code = 'FINISH' THEN '已完成'
        WHEN t1.process_stat_code = 'CANCEL' THEN '已取消'
        WHEN t1.process_stat_code = 'PARTIAL_FINISH' THEN '部分中止'
        WHEN t1.process_stat_code = 'OA_PARTIAL_FINISH' THEN '完全终止'
        WHEN t1.process_stat_code = 'WAIT_PAY' THEN '待付款'
        WHEN t1.process_stat_code = 'REFUNDING' THEN '退款中'
        WHEN t1.process_stat_code = 'REFUND_FINISH' THEN '退款完成'
        WHEN t1.process_stat_code = 'REFUND_FAILURE' THEN '退款驳回'
        WHEN t1.process_stat_code = 'AUTO_REFUND_FAILURE' THEN '自动退款失败'
        ELSE 'UNKNOWN'
    END as process_status,
    CASE WHEN is_salesman_order = 'Y' THEN 1
         ELSE 0
    END as is_salesman_order,
    t1.salesman_id,
    t4.salesman_name,
    CAST(ORDER_TOTAL_AMOUNT / 10000 as decimal(18,4)) as sales_amount
FROM ods_dsys.prd_order t1
LEFT JOIN dim.entry t2 ON t1.org_id = t2.org_id
LEFT JOIN dim.eshop_customer_buyer t3 
    ON t1.buyers_id = t3.buyers_id AND t1.create_time >= t3.dw_starttime AND t1.create_time < t3.dw_endtime
LEFT JOIN (SELECT salesman_id, MIN(salesman_name) as salesman_name FROM ods_dsys.sys_org_salesman GRUOP BY salesman_id) t4
    ON t1.salesman_id = t4.salesman_id;

CREATE INDEX IF NOT EXISTS idx_create_time ON dwd.eshop_order_sales_doc (create_time);
CREATE INDEX IF NOT EXISTS idx_entryid ON dwd.eshop_order_sales_doc (entryid);
CREATE INDEX IF NOT EXISTS idx_customid ON dwd.eshop_order_sales_doc (customid);
CREATE INDEX IF NOT EXISTS idx_buyers_id ON dwd.eshop_order_sales_doc (buyers_id);
CREATE INDEX IF NOT EXISTS idx_salesman_id ON dwd.eshop_order_sales_doc (salesman_id);
CREATE INDEX IF NOT EXISTS idx_process_stat_code ON dwd.eshop_order_sales_doc (process_stat_code);