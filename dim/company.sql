DROP TABLE IF EXISTS dim.company;
CREATE TABLE dim.company (
  companyid bigint COMMENT '公司ID',
  dw_starttime datetime COMMENT '数据生效时间',
  dw_endtime datetime COMMENT '数据失效时间',
  is_active tinyint COMMENT '是否有效',
  company_name varchar COMMENT '公司名称'
) UNIQUE KEY(companyid, dw_starttime) DISTRIBUTED BY HASH(companyid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);
INSERT INTO
  dim.company (
    companyid,
    dw_starttime,
    dw_endtime,
    is_active,
    company_name
  ) WITH ranked_company AS (
    SELECT
      companyid,
      dw_createtime,
      dw_updatetime,
      is_active,
      companyname,
      ROW_NUMBER() OVER (
        PARTITION BY companyid
        ORDER BY
          dw_createtime
      ) AS record_seq,
      LEAD (date (dw_createtime), 1, NULL) OVER (
                PARTITION BY
                    companyid
                ORDER BY
                    dw_createtime
            ) AS next_start_time
    FROM
      ods_erp.pub_company
  )
SELECT
  companyid,
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
  companyname
FROM
  ranked_company;

-- 添加索引优化查询性能
CREATE INDEX IF NOT EXISTS idx_startdates ON dim.company (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.company (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.company (is_active);