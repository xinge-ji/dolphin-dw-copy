MODEL (
  name sqlmesh_dim.erp_pub_goods_price,
  kind FULL,
  dialect doris,
  grain (valid_from, valid_to, priceid, goodsid),
  physical_properties (
    unique_key = (valid_from, valid_to, priceid, goodsid),
    distributed_by = (kind='HASH', expressions=priceid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "商品价格维度表"
);

WITH ranked_data AS (
    SELECT priceid, goodsid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY priceid, goodsid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    priceid, goodsid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_goods_price
)
SELECT
  CASE
    WHEN r.record_seq = 1 
    THEN DATE('1970-01-01')
    ELSE date(a.DW_CREATETIME)
  END AS valid_from,
  CASE
    WHEN r.next_start_time IS NOT NULL THEN r.next_start_time
    ELSE DATE('9999-12-31')
  END AS valid_to,
  a.priceid,
  a.goodsid,
  b.opcode AS priceopcode,
  b.pricename,
  c.opcode,
  c.goodsname,
  c.currencyname,
  c.goodsno,
  c.goodstype,
  c.goodsunit,
  c.prodarea,
  c.factoryid,
  d.factoryname,
  a.price,
  a.discount,
  a.refrencepriceid,
  e.opcode AS refrencepriceopcode,
  e.pricename AS refrencepricename,
  a.refrenceprice,
  a.gdspriceid,
  a.usestatus,
  IFNULL(b.discountflag,0) AS discountflag
FROM ranked_data r
JOIN ods_erp.pub_goods_price a ON r.priceid=a.priceid AND r.goodsid=a.goodsid AND r.DW_CREATETIME=a.DW_CREATETIME
JOIN sqlmesh_dim.erp_pub_price_type b ON a.priceid = b.priceid AND b.valid_to = DATE('9999-12-31')
JOIN sqlmesh_dim.erp_pub_goods c ON a.goodsid = c.goodsid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_factory d ON c.factoryid = d.factoryid AND d.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_price_type e ON a.refrencepriceid = e.priceid AND e.valid_to = DATE('9999-12-31'); 