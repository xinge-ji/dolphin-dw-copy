-- 插入最近60天的审批数据
INSERT INTO dwd.wholesale_order_approval (
    salesid,
    entryid,
    submit_time,
    notify_time,
    approval_time,
    entry_name,
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
    a.billno,
    b.display_name as approval_node,
    soi.jl_name as approval_name,
    wal.comment as approval_comment
FROM ods_oa.wf_action_log wal
JOIN ods_oa.sys_org_item soi ON wal.operator_id = soi.jl_id
JOIN ods_oa.sys_org_item soi1 ON soi.jl_parent = soi1.jl_id
JOIN ods_oa.t_cqcekd_doc a ON wal.billid = a.billid
JOIN ods_oa.wf_hist_task b ON wal.task_id = b.id
WHERE wal.task_id IS NOT NULL
AND wal.is_active = 1
AND soi.is_active = 1
AND a.is_active = 1
AND b.is_active = 1
AND (a.sqrq >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
     OR b.create_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
     OR b.finish_Time >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY));