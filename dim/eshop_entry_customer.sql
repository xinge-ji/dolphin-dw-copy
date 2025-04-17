DROP TABLE IF EXISTS dim.eshop_entry_customer;
CREATE TABLE dim.eshop_entry_customer (
        entryid bigint COMMENT '独立单元ID',
        customid bigint COMMENT '客户ID',
        dw_starttime datetime COMMENT '客户上线日期',
        dw_endtime datetime COMMENT '客户下线日期',
        org_id bigint COMMENT '独立单元电商ID',
        buyers_id bigint COMMENT '客户电商ID'
    ) UNIQUE KEY (entryid, customid) DISTRIBUTED BY HASH (customid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );


-- 插入数据到dim.eshop_entry_customer表
INSERT INTO dim.eshop_entry_customer (
    entryid,
    customid,
    buyers_id,
    org_id,
    dw_starttime,
    dw_endtime
)
SELECT 
    t1.entryid,
    t1.customid,
    MAX(t3.buyers_id),
    MAX(t3.org_id),
    MIN(IFNULL(t1.credate, t3.create_time)) AS dw_starttime,
    CASE 
        WHEN MAX(CASE WHEN t1.is_active = 1 THEN 1 ELSE 0 END) = 1 THEN 
            CAST('9999-12-31 23:59:59' AS DATETIME)
        ELSE 
            MAX(t1.dw_updatetime) 
    END AS dw_endtime
FROM ods_erp.eshop_entry_customer t1
LEFT JOIN ods_dsys.sys_buyers t2 on t1.customid = t2.ERP_CODE
LEFT JOIN (SELECT entryid, MAX(org_id) as org_id FROM dim.entry group by entryid) e ON t1.entryid = e.entryid
LEFT JOIN ods_dsys.sys_buyers_org_rel t3 ON t3.BUYERS_ID = t2.BUYERS_ID AND e.org_id = t3.org_id
WHERE t1.entryid is not null and t1.customid is not null
GROUP BY t1.entryid, t1.customid;

CREATE INDEX IF NOT EXISTS idx_eshop_entry_customer_entryid ON dim.eshop_entry_customer (entryid);
CREATE INDEX IF NOT EXISTS idx_eshop_entry_customer_customid ON dim.eshop_entry_customer (customid);
CREATE INDEX IF NOT EXISTS idx_eshop_entry_customer_buyers_id ON dim.eshop_entry_customer (buyers_id);
CREATE INDEX IF NOT EXISTS idx_eshop_entry_customer_org_id ON dim.eshop_entry_customer (org_id);

