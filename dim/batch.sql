DROP TABLE IF EXISTS dim.batch;

CREATE TABLE
  dim.batch (
    batchid bigint COMMENT '批次ID',
    dw_starttime datetime COMMENT '数据生效时间',
    dw_endtime datetime COMMENT '数据失效时间',
    is_active tinyint COMMENT '是否有效',
    goodsid bigint COMMENT '商品ID',
    goodsdtlid bigint COMMENT '商品明细ID',
    batchno varchar COMMENT '批次号',
    create_date datetime COMMENT '建立日期',
    notax_price decimal(18, 10) COMMENT '不含税单价',
    unit_price decimal(20, 10) COMMENT '含税单价',
    companyid bigint COMMENT '公司ID',
    company_name varchar COMMENT '公司名称'
  ) UNIQUE KEY (batchid, dw_starttime) DISTRIBUTED BY HASH (batchid) PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
  );

INSERT INTO
  dim.batch (
    batchid,
    dw_starttime,
    dw_endtime,
    is_active,
    goodsid,
    goodsdtlid,
    batchno,
    create_date,
    notax_price,
    unit_price,
    companyid,
    company_name
  )
WITH
  ranked_batch AS (
    SELECT
      batchid,
      dw_createtime,
      dw_updatetime,
      is_active,
      goodsid,
      goodsdtlid,
      batchno,
      credate,
      notaxsuprice,
      unitprice,
      companyid,
      ROW_NUMBER() OVER (
        PARTITION BY
          batchid
        ORDER BY
          dw_createtime
      ) AS record_seq,
      LEAD (date (dw_createtime), 1, NULL) OVER (
        PARTITION BY
          batchid
        ORDER BY
          dw_createtime
      ) AS next_start_time
    FROM
      ods_erp.bms_batch_def
  )
SELECT
  t1.batchid,
  CASE
    WHEN t1.record_seq = 1 THEN LEAST (date ('1970-01-01'), date (t1.dw_createtime))
    ELSE date (t1.dw_createtime)
  END AS dw_starttime,
  CASE
    WHEN t1.next_start_time IS NOT NULL THEN t1.next_start_time
    WHEN t1.dw_createtime <> t1.dw_updatetime THEN date (t1.dw_updatetime)
    ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
  END AS dw_endtime,
  CASE
    WHEN t1.next_start_time IS NOT NULL THEN 0
    WHEN t1.dw_createtime <> t1.dw_updatetime THEN 0
    ELSE 1
  END AS is_active,
  t1.goodsid,
  t1.goodsdtlid,
  t1.batchno,
  t1.credate,
  t1.notaxsuprice,
  t1.unitprice,
  t1.companyid,
  t2.company_name
FROM
  ranked_batch t1
  LEFT JOIN dim.company t2 ON t1.companyid = t2.companyid
WHERE
  t2.is_active = 1;

CREATE INDEX IF NOT EXISTS idx_startdates ON dim.batch (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.batch (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.batch (is_active);