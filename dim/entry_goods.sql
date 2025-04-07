DROP TABLE IF EXISTS dim.entry_goods_taxrate;
CREATE TABLE dim.entry_goods_taxrate (
  seqid bigint COMMENT '序列ID',
  entryid BIGINT COMMENT '独立单元ID',
  goodsid bigint COMMENT '商品ID',
  dw_starttime datetime COMMENT '数据开始时间',
  dw_endtime datetime COMMENT '数据结束时间',
  is_active tinyint COMMENT '是否有效',
  customid bigint COMMENT '客户ID',
  supplyid bigint COMMENT '供应商ID',
  sales_taxrate decimal(10,4) COMMENT '销售税率',
  supply_taxrate decimal(10,4) COMMENT '采购税率'
) UNIQUE KEY(seqid,entryid,goodsid, dw_starttime) DISTRIBUTED BY HASH(seqid,entryid,goodsid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
      "in_memory" = "false",
      "storage_format" = "V2",
      "disable_auto_compaction" = "false"
);
INSERT INTO
  dim.entry_goods_taxrate (
    seqid,
    entryid,
    goodsid,
    dw_starttime,
    dw_endtime,
    is_active,
    customid,
    supplyid,
    sales_taxrate,
    supply_taxrate
  ) WITH ranked_entry_goods_taxrate AS (
    SELECT
      seqid,
      entryid,
      goodsid,
      dw_createtime,
      dw_updatetime,
      is_active,
      customid,
      supplyid,
      salestaxrate,
      supplytaxrate,
      ROW_NUMBER() OVER (
        PARTITION BY seqid,entryid,goodsid
        ORDER BY
          dw_createtime
      ) AS record_seq,
      LEAD(date(dw_createtime),1,NULL) OVER (
        PARTITION BY seqid,entryid,goodsid
          ORDER BY dw_createtime
      ) AS next_start_time
    FROM
      ods_erp.Zx_Pub_Taxrate
  )
SELECT
  seqid,
  entryid,
  goodsid,
  CASE
    WHEN record_seq = 1 THEN LEAST(date('1970-01-01'), date(dw_createtime))
    ELSE date(dw_createtime)
  END AS dw_starttime,
  CASE
    WHEN next_start_time IS NOT NULL THEN next_start_time
    WHEN dw_createtime <> dw_updatetime THEN dw_updatetime
    ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
  END AS dw_endtime,
  CASE
    WHEN next_start_time IS NOT NULL THEN 0
    WHEN dw_createtime <> dw_updatetime THEN 0
    ELSE 1
  END AS is_active,
  customid,
  supplyid,
  salestaxrate,
  supplytaxrate
FROM
  ranked_entry_goods_taxrate;

CREATE INDEX idx_startdates ON dim.entry_goods_taxrate (dw_starttime);
CREATE INDEX idx_enddates ON dim.entry_goods_taxrate (dw_endtime);
CREATE INDEX idx_active ON dim.entry_goods_taxrate (is_active);