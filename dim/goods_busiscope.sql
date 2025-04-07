DROP TABLE IF EXISTS dim.goods_busiscope;

CREATE TABLE
  dim.goods_busiscope (
    keyid bigint COMMENT '主键',
    dw_starttime datetime COMMENT '数据开始时间',
    dw_endtime datetime COMMENT '数据结束时间',
    is_active tinyint COMMENT '是否有效',
    scopedefid bigint COMMENT '业务范围id',
    scope_name varchar(255) COMMENT '业务范围名称',
    nianbao_type varchar(255) COMMENT '年报类型'
  ) UNIQUE KEY (keyid, dw_starttime) DISTRIBUTED BY HASH (keyid) PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
  );

INSERT INTO
  dim.goods_busiscope (
    keyid,
    dw_starttime,
    dw_endtime,
    is_active,
    scopedefid,
    scope_name,
    nianbao_type
  )
WITH
  ranked_goods_busiscope AS (
    SELECT
      keyid,
      dw_createtime,
      dw_updatetime,
      is_active,
      scopedefid,
      scopename,
      lbfl,
      ROW_NUMBER() OVER (
        PARTITION BY
          keyid
        ORDER BY
          dw_createtime
      ) AS record_seq,
      LEAD (date (dw_createtime),1,NULL) OVER (
        PARTITION BY
          keyid
        ORDER BY
          dw_createtime
      ) AS next_start_time
    FROM
      ods_erp.bms_busiscope_def
  )
SELECT
  CAST(t1.keyid as bigint),
  CASE
    WHEN t1.record_seq = 1 THEN LEAST(date('1970-01-01'), date(t1.dw_createtime))
    ELSE date(t1.dw_createtime)
  END AS dw_starttime,
  CASE
    WHEN t1.next_start_time IS NOT NULL THEN t1.next_start_time
    WHEN t1.dw_createtime <> t1.dw_updatetime THEN date(t1.dw_updatetime)
    ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
  END AS dw_endtime,
  CASE
    WHEN t1.next_start_time IS NOT NULL THEN 0
    WHEN t1.dw_createtime <> t1.dw_updatetime THEN 0
    ELSE 1
  END AS is_active,
  t1.scopedefid,
  t1.scopename,
  t2.ddlname
FROM
  ranked_goods_busiscope t1
  LEFT JOIN (
    Select
      Ddlid,
      MIN(Ddlname) as ddlname
    From
      ods_erp.Pub_Ddl_Dtl
    Where
      Sysid = 100866
      and is_active = 1
    group by
      ddlid
  ) t2 ON t1.lbfl = t2.ddlid;

CREATE INDEX IF NOT EXISTS idx_startdates ON dim.goods_busiscope (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.goods_busiscope (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.goods_busiscope (is_active);