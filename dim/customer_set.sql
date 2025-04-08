DROP TABLE IF EXISTS dim.customer_set;

CREATE TABLE
    dim.customer_set (
        setdtlid bigint COMMENT '分组明细ID',
        dw_starttime datetime COMMENT '数据开始时间',
        dw_endtime datetime COMMENT '数据结束时间',
        is_active tinyint COMMENT '是否有效',
        setid bigint COMMENT '分组ID',
        customid varchar COMMENT '客户ID'
    ) UNIQUE KEY (setdtlid, dw_starttime) DISTRIBUTED BY HASH (setdtlid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO
    dim.customer_set (
        setdtlid,
        dw_starttime,
        dw_endtime,
        is_active,
        setid,
        customid
    )
WITH
    ranked_customer_set AS (
        SELECT
            setdtlid,
            dw_createtime,
            dw_updatetime,
            is_active,
            setid,
            customid,
            ROW_NUMBER() OVER (
                PARTITION BY
                    setdtlid
                ORDER BY
                    dw_createtime
            ) AS record_seq,
            LEAD (date (dw_createtime), 1, NULL) OVER (
                PARTITION BY
                    setdtlid
                ORDER BY
                    dw_createtime
            ) AS next_start_time
        FROM
            ods_erp.pub_custom_set_dtl
    )
SELECT
    setdtlid,
    CASE
      WHEN record_seq = 1 THEN LEAST(date('1970-01-01'), date(dw_createtime))
      ELSE date(dw_createtime)
    END AS dw_starttime,
    CASE
      WHEN next_start_time IS NOT NULL THEN next_start_time
      WHEN dw_createtime <> dw_updatetime THEN date(dw_updatetime)
      ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
    END AS dw_endtime,
    CASE
      WHEN next_start_time IS NOT NULL THEN 0
      WHEN dw_createtime <> dw_updatetime THEN 0
      ELSE 1
    END AS is_active,
    setid,
    customid
FROM
    ranked_customer_set;

-- 添加索引优化查询性能
CREATE INDEX IF NOT EXISTS idx_startdates ON dim.customer_set (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.customer_set (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.customer_set (is_active);