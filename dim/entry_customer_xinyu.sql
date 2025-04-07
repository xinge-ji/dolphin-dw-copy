DROP TABLE IF EXISTS dim.entry_customer_xinyu;

CREATE TABLE
  dim.entry_customer_xinyu (
    seqid bigint comment '主键',
    entryid bigint comment '独立单元id',
    customid bigint comment '客户id',
    dw_starttime datetime COMMENT '数据生效时间',
    dw_endtime datetime COMMENT '数据失效时间',
    is_active tinyint COMMENT '是否有效',
    guankong_type int comment '管控类型',
    xinyu_days int comment '信誉天数',
    xinyu_months int comment '信誉月数',
    reputation_days int comment '实际信誉天数'
  ) UNIQUE KEY (seqid, entryid, customid, dw_starttime) DISTRIBUTED BY HASH (entryid) PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
  );

INSERT INTO
  dim.entry_customer_xinyu (
    seqid,
    entryid,
    customid,
    dw_starttime,
    dw_endtime,
    is_active,
    guankong_type,
    xinyu_days,
    xinyu_months,
    reputation_days
  )
WITH
  ranked_t_12102 AS (
    SELECT
      seqid,
      entryid,
      customid,
      dw_createtime,
      dw_updatetime,
      is_active,
      gklx,
      xyts,
      xytszry,
      ROW_NUMBER() OVER (
        PARTITION BY
          seqid,
          entryid,
          customid
        ORDER BY
          dw_createtime
      ) as record_seq,
      LEAD (date (dw_createtime), 1, NULL) OVER (
        PARTITION BY
          seqid,
          entryid,
          customid
        ORDER BY
          dw_createtime
      ) AS next_start_time
    FROM
      ods_erp.t_12102
  )
SELECT
  seqid,
  entryid,
  customid,
  CASE
    WHEN record_seq = 1 THEN LEAST (date ('1970-01-01'), date (dw_createtime))
    ELSE date (dw_createtime)
  END AS dw_starttime,
  CASE
    WHEN next_start_time IS NOT NULL THEN next_start_time
    WHEN dw_createtime <> dw_updatetime THEN date (dw_updatetime)
    ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
  END AS dw_endtime,
  CASE
    WHEN next_start_time IS NOT NULL THEN 0
    WHEN dw_createtime <> dw_updatetime THEN 0
    ELSE 1
  END AS is_active,
  gklx,
  xyts,
  xytszry,
  CASE
    WHEN gklx = 2 THEN xyts
    WHEN gklx = 1 THEN xytszry
    ELSE NULL
  END AS actual_xinyu_days
FROM
  ranked_t_12102;

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_startdates ON dim.entry_customer_xinyu (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.entry_customer_xinyu (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.entry_customer_xinyu (is_active);