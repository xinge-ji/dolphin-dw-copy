DROP TABLE IF EXISTS dim.wholesale_task_set;
CREATE TABLE dim.wholesale_task_set (
    entryid bigint COMMENT '独立单元ID',
    customid bigint COMMENT '客户ID',
    goods_set_id bigint COMMENT '商品目录ID',
    docid bigint COMMENT '项目ID',
    task_name varchar(255) COMMENT '任务名称',
    goods_count int COMMENT '商品数量'
) UNIQUE KEY (entryid, customid, goods_set_id, docid) DISTRIBUTED BY HASH (docid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO dim.wholesale_task_set (
    docid,
    task_name,
    entryid,
    customid,
    goods_set_id,
    goods_count
)
WITH task_customers AS (
    -- 当khjhid不为空时，使用客户集合中的客户
    SELECT 
        a.docid,
        a.xmmc,
        a.entryid AS task_entryid,
        a.hpjhid,
        a.pckhjhid,
        cs.customid
    FROM 
        ods_erp.t_101186_doc a
    JOIN 
        ods_erp.pub_custom_set_dtl cs ON cs.setid = a.khjhid
    WHERE 
        a.khjhid IS NOT NULL
        AND cs.is_active = 1
        AND a.is_active = 1
    
    UNION ALL
    
    -- 当khjhid为空时，使用所有活跃的客户
    SELECT 
        a.docid,
        a.xmmc,
        a.entryid AS task_entryid,
        a.hpjhid,
        a.pckhjhid,
        c.customid
    FROM 
        ods_erp.t_101186_doc a
    CROSS JOIN 
        dim.customer c
    WHERE 
        a.khjhid IS NULL
        AND c.is_active = 1
        AND a.is_active = 1
),
goods_count AS (
    SELECT
        a.docid,
        COUNT(DISTINCT gs.goodsid) AS goods_count
    FROM
        ods_erp.t_101186_doc a
    JOIN
        dim.goods_set gs ON a.hpjhid = gs.setid
    WHERE
        a.is_active = 1
    GROUP BY
        a.docid
)
SELECT
    tc.docid,
    tc.xmmc as task_name,
    e.entryid,
    tc.customid,
    tc.hpjhid as goods_set_id,
    gc.goods_count
FROM
    task_customers tc
CROSS JOIN
    dim.entry e
LEFT JOIN
    goods_count gc ON tc.docid = gc.docid
WHERE
    NOT EXISTS (
        SELECT 1
        FROM ods_erp.pub_custom_set_dtl pcs
        WHERE pcs.setid = tc.pckhjhid
        AND pcs.customid = tc.customid
        AND pcs.is_active = 1 
    ) -- 排除客户
    AND e.is_active = 1 -- 只使用有效的独立单元
    AND e.area_name != 'UNKNOWN' -- 只展示分销数据
    -- AND gs.is_active = 1 -- 只使用有效的商品集合
    AND (tc.task_entryid = 0 OR e.entryid = tc.task_entryid OR
        (tc.task_entryid = 25 AND e.area_name = '江西'));


