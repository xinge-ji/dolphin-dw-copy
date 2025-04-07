DROP TABLE IF EXISTS dim.storage;
CREATE TABLE dim.storage (
  storageid bigint COMMENT '保管帐ID',
  dw_starttime datetime COMMENT '数据开始时间',
  dw_endtime datetime COMMENT '数据结束时间',
  is_active tinyint COMMENT '是否有效',
  storage_name varchar COMMENT '保管帐名称'
) UNIQUE KEY(storageid, dw_starttime) DISTRIBUTED BY HASH(storageid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);
INSERT INTO
  dim.storage (
    storageid,
    dw_starttime,
    dw_endtime,
    is_active,
    storage_name
  ) WITH ranked_storage AS (
    SELECT
      storageid,
      dw_createtime,
      dw_updatetime,
      is_active,
      storagename,
      ROW_NUMBER() OVER (
        PARTITION BY storageid
        ORDER BY
          dw_createtime
      ) AS record_seq,
      LEAD(date(dw_createtime),1,NULL) OVER (
      	PARTITION BY storageid
          ORDER BY dw_createtime
      ) AS next_start_time
    FROM
      ods_erp.bms_st_def
  )
SELECT
  CAST(storageid as bigint),
  dw_id,
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
  storagename
FROM
  ranked_storage;

-- 添加索引优化查询性能
CREATE INDEX IF NOT EXISTS idx_startdates ON dim.storage (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.storage (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.storage (is_active);