DROP TABLE IF EXISTS dwd.wholesale_order_approval;
CREATE TABLE dwd.wholesale_order_approval(
    salesid bigint COMMENT '销售单id',
    submit_time datetime COMMENT '送审时间', 
    notify_time datetime COMMENT '通知时间',
    approval_time datetime COMMENT '审批时间',
    entryid bigint COMMENT '独立单元id',
    entry_name varchar COMMENT '独立单元名称',
    customid bigint COMMENT '客户id',
    customer_name varchar COMMENT '客户名称',
    billno varchar COMMENT '单号',
    approval_node varchar COMMENT '审批节点',
    approval_name varchar COMMENT '审批人',
    approval_comment varchar COMMENT '审批意见'
)
UNIQUE KEY(salesid, submit_time, notify_time, approval_time) 
DISTRIBUTED BY HASH(salesid) 
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
);

-- 插入审批数据
INSERT INTO dwd.wholesale_order_approval (
    salesid,
    entryid,
    submit_time,
    notify_time,
    approval_time,
    entry_name,
    customid,
    customer_name,
    billno,
    approval_node,
    approval_name,
    approval_comment
)
SELECT
    a.salesid,
    a.entryid,
    a.sqrq as submit_time,
    b.create_time as notify_time,
    b.finish_Time as approval_time,
    a.entryname as entry_name,
    wos.customid,
    wos.customer_name,
    a.billno,
    b.display_name as approval_node,
    soi.jl_name as approval_name,
    wal.comment as approval_comment
FROM ods_oa.wf_action_log wal
JOIN ods_oa.sys_org_item soi ON wal.operator_id = soi.jl_id
JOIN ods_oa.sys_org_item soi1 ON soi.jl_parent = soi1.jl_id
JOIN ods_oa.t_cqcekd_doc a ON wal.billid = a.billid
JOIN ods_oa.wf_hist_task b ON wal.task_id = b.id
LEFT JOIN dwd.wholesale_order_sales wos ON a.salesid = wos.salesid
WHERE wal.task_id IS NOT NULL
AND wal.is_active = 1
AND soi.is_active = 1
AND a.is_active = 1
AND b.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_order_approval_entryid ON dwd.wholesale_order_approval (entryid);
CREATE INDEX IF NOT EXISTS idx_order_approval_billno ON dwd.wholesale_order_approval (billno);