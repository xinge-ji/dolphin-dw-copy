MODEL (
  name sqlmesh_dim.erp_gpcs_subshop_price,
  kind FULL,
  dialect doris,
  grain (placepointid, goodsid, priceid),
  physical_properties (
    unique_key = (valid_from, valid_to, placepointid, goodsid, priceid),
    distributed_by = (kind='HASH', expressions=placepointid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "门店价格维度表"
);

WITH ranked_data AS (
    SELECT placepointid, goodsid, priceid, priceunit, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY placepointid, goodsid, priceid, priceunit
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    placepointid, goodsid, priceid, priceunit
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.gpcs_subshop_price
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
  a.placepointid,
  a.goodsid,
  a.priceid,
  a.priceunit,
  b.placepointopcode,
  b.placepointname,
  c.opcode AS goodsopcode,
  c.goodsname,
  c.goodstype,
  c.prodarea,
  c.goodsunit,
  c.factoryid AS factid,
  c.factoryname,
  d.opcode AS priceopcode,
  d.pricename,
  a.price
FROM ranked_data r
JOIN ods_erp.gpcs_subshop_price a ON r.placepointid = a.placepointid AND r.goodsid = a.goodsid AND r.priceid = a.priceid AND r.DW_CREATETIME = a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_gpcs_placepoint b ON a.placepointid = b.placepointid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_goods c ON a.goodsid = c.goodsid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_price_type d ON a.priceid = d.priceid AND d.valid_to = DATE('9999-12-31'); 