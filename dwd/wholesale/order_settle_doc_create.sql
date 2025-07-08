DROP TABLE IF EXISTS dwd.wholesale_order_settle_doc;
CREATE TABLE dwd.wholesale_order_settle_doc (
	sasettleid bigint COMMENT '订单结算单id',
    dw_updatetime datetime COMMENT '更新时间',
    create_date datetime COMMENT '创建时间',
    confirm_date datetime COMMENT '确认时间',  
    use_status varchar COMMENT '使用状态: 作废/正式/临时',
    entryid bigint COMMENT '独立单元id',
    customid bigint COMMENT '客户id',
    -- 人员维度
    inputmanid bigint COMMENT '制单人id',
    inputman_name varchar COMMENT '制单人名称'
)
UNIQUE KEY(sasettleid) DISTRIBUTED BY HASH(sasettleid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 一次性插入所有数据，整合了原来的UPDATE操作
INSERT INTO dwd.wholesale_order_settle_doc (
    sasettleid,
    dw_updatetime,
    create_date,
    confirm_date,
    use_status,
    entryid,
    customid,
    inputmanid,
    inputman_name
)
SELECT
    a.sasettleid,                                      -- 订单结算单id
    a.dw_updatetime,                                   -- 更新时间
    IFNULL(a.credate, a.confirmdate) AS create_date,   -- 创建时间
    a.confirmdate,                                     -- 确认时间
    CASE
    	WHEN a.usestatus=0 THEN '作废'
        WHEN a.usestatus=1 THEN '正式'
        WHEN a.usestatus=2 THEN '临时'
        ELSE 'UNKNOWN'
    END AS use_status,                          -- 使用状态
    a.entryid,                                  -- 独立单元id
    a.customid,                                 -- 客户id
    a.inputmanid,                               -- 制单人id
    e.employee_name                             -- 制单人名称
FROM 
    ods_erp.bms_sa_settle_doc a                 -- 结算单主表
LEFT JOIN 
    dim.employee e ON a.inputmanid = e.employeeid
    AND IFNULL(a.credate, a.confirmdate) >= e.dw_starttime AND IFNULL(a.credate, a.confirmdate) < e.dw_endtime  -- 员工维度表
WHERE 
    a.is_active=1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_settle_doc_entryid ON dwd.wholesale_order_settle_doc (entryid);
CREATE INDEX IF NOT EXISTS idx_settle_doc_customid ON dwd.wholesale_order_settle_doc (customid);
CREATE INDEX IF NOT EXISTS idx_settle_doc_create_date ON dwd.wholesale_order_settle_doc (create_date);