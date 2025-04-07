DROP TABLE IF EXISTS dim.eshop_customer_salesman;
CREATE TABLE dim.eshop_customer_salesman (
        entryid bigint COMMENT '独立单元ID',
        customid bigint COMMENT '客户ID',
        salesman_id bigint COMMENT '销售员ID',
        buyers_base_id bigint COMMENT '客户id对应电商id', 
        org_id bigint COMMENT '独立单元id对应电商id',
        salesman_name varchar COMMENT '销售员姓名',
        dw_starttime datetime COMMENT '数据开始时间',
        dw_endtime datetime COMMENT '数据结束时间'
    ) UNIQUE KEY (entryid, customid, salesman_id) DISTRIBUTED BY HASH (customid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

-- 插入数据到dim.eshop_customer_salesman表
INSERT INTO dim.eshop_customer_salesman (
    entryid,
    customid,
    salesman_id,
    buyers_base_id,
    org_id,
    salesman_name,
    dw_starttime,
    dw_endtime
)
WITH salesman_rel AS (
    -- 获取客户与销售员的关系信息
    SELECT 
        rel.buyers_id,
        rel.org_id,
        rel.buyers_salesman_id AS salesman_id,
        rel.create_time AS rel_starttime,
        CASE 
            WHEN rel.is_enable = 'N' THEN MIN(rel.update_time) OVER(PARTITION BY rel.buyers_id, rel.org_id, rel.buyers_salesman_id)
            ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
        END AS rel_endtime
    FROM ods_dsys.sys_buyers_salesman_rel rel
),
buyers_base_mapping AS (
    -- 获取buyers_id与buyers_base_id的映射关系
    SELECT 
        buyers_id,
        buyers_base_id
    FROM ods_dsys.sys_buyers
)
-- 关联数据并插入
SELECT 
    ec.entryid,
    ec.customid,
    sr.salesman_id,
    ec.buyers_base_id,
    ec.org_id,
    sos.salesman_name,
    GREATEST(ec.dw_starttime, sr.rel_starttime) AS dw_starttime, -- 取两个时间的较大值作为实际开始时间
    LEAST(ec.dw_endtime, sr.rel_endtime) AS dw_endtime -- 取两个时间的较小值作为实际结束时间
FROM dim.eshop_entry_customer ec
JOIN buyers_base_mapping bbm ON ec.buyers_base_id = bbm.buyers_base_id
JOIN salesman_rel sr ON bbm.buyers_id = sr.buyers_id AND ec.org_id = sr.org_id
JOIN ods_dsys.sys_org_salesman sos ON sr.salesman_id = sos.salesman_id
WHERE GREATEST(ec.dw_starttime, sr.rel_starttime) < LEAST(ec.dw_endtime, sr.rel_endtime); -- 确保时间段有效

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_ecs_starttime ON dim.eshop_customer_salesman (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_ecs_endtime ON dim.eshop_customer_salesman (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_ecs_customid ON dim.eshop_customer_salesman (customid);
CREATE INDEX IF NOT EXISTS idx_ecs_entryid ON dim.eshop_customer_salesman (entryid);
CREATE INDEX IF NOT EXISTS idx_ecs_salesman ON dim.eshop_customer_salesman (salesman_id);