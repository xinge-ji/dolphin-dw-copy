DROP TABLE IF EXISTS dim.price_type;

CREATE TABLE
    dim.price_type (
        priceid bigint COMMENT '价格id',
        dw_starttime datetime COMMENT '数据开始时间',
        dw_endtime datetime COMMENT '数据结束时间',
        is_active tinyint COMMENT '是否有效',
        price_name varchar COMMENT '价格名称'
    ) UNIQUE KEY (priceid, dw_starttime) DISTRIBUTED BY HASH (priceid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO
    dim.price_type (
        priceid,
        dw_starttime,
        dw_endtime,
        is_active,
        price_name
    )
WITH
    ranked_price_type AS (
        SELECT
            priceid,
            dw_createtime,
            dw_updatetime,
            is_active,
            pricename,
            ROW_NUMBER() OVER (
                PARTITION BY
                    priceid
                ORDER BY
                    dw_createtime
            ) AS record_seq,
            LEAD (date (dw_createtime), 1, NULL) OVER (
                PARTITION BY
                    priceid
                ORDER BY
                    dw_createtime
            ) AS next_start_time
        FROM
            ods_erp.pub_price_type
    )
SELECT
    priceid,
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
    pricename
FROM
    ranked_price_type;


CREATE INDEX IF NOT EXISTS idx_startdates ON dim.price_type (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.price_type (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.price_type (is_active);
