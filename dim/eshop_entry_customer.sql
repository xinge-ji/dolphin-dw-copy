DROP TABLE IF EXISTS dim.eshop_entry_customer;
CREATE TABLE dim.eshop_entry_customer (
        entryid bigint COMMENT '独立单元ID',
        customid bigint COMMENT '客户ID',
        dw_starttime datetime COMMENT '客户上线日期',
        dw_endtime datetime COMMENT '客户下线日期',
        org_id bigint COMMENT '独立单元电商ID',
        buyers_base_id bigint COMMENT '客户电商ID'
    ) UNIQUE KEY (entryid, customid, dw_starttime) DISTRIBUTED BY HASH (customid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );


-- 插入数据到dim.eshop_entry_customer表
INSERT INTO dim.eshop_entry_customer (
    entryid,
    customid,
    dw_starttime,
    dw_endtime,
    org_id,
    buyers_base_id
)
WITH
    ranked_eshop_entry_customer AS (
        SELECT
            entryid,
            customid,
            tob2bdate,
            MAX(dw_createtime) as dw_createtime,
            MAX(dw_updatetime) as dw_updatetime
        FROM
            ods_erp.eshop_entry_customer
        WHERE
            entryid IS NOT NULL
            AND customid IS NOT NULL
            AND tob2bdate IS NOT NULL
        GROUP BY
            entryid,
            customid,
            tob2bdate
    )
SELECT
    ec.entryid,
    ec.customid,
    ec.tob2bdate as dw_starttime,
    CASE
      WHEN ec.dw_createtime <> ec.dw_updatetime THEN date(ec.dw_updatetime)
      ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
    END as dw_endtime,
    e.org_id,
    t.buers_base_id
FROM
    ranked_eshop_entry_customer ec
LEFT JOIN
    (SELECT entryid, MIN(org_id) as org_id FROM dim.entry group by entryid) e ON ec.entryid = e.entryid
LEFT JOIN
    (SELECT ERP_BASE_CODE, MIN(buyers_base_id) as buyers_base_id FROM ods_dsys.sys_buyers_base group by ERP_BASE_CODE) t ON ec.customid = t.ERP_BASE_CODE;


CREATE INDEX IF NOT EXISTS idx_startdates ON dim.eshop_entry_customer (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.eshop_entry_customer (dw_endtime);
