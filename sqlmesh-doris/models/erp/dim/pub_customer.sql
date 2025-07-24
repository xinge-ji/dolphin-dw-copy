MODEL (
  name sqlmesh_dim.erp_pub_customer,
  kind FULL,
  dialect doris,
  grain customid,
  physical_properties (
    unique_key = (valid_from, valid_to, customid),
    distributed_by = (kind='HASH', expressions=customid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "客户维度表"
);

WITH ranked_data AS (
    SELECT CUSTOMID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY CUSTOMID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    CUSTOMID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_customer
)
SELECT 
CASE
  WHEN a.record_seq = 1 
  THEN DATE('1970-01-01')
  ELSE date(b.DW_CREATETIME)
END AS valid_from,
CASE
  WHEN a.next_start_time IS NOT NULL THEN a.next_start_time
  ELSE DATE('9999-12-31')
END AS valid_to,
  b.customid,
  b.customopcode,
  b.customno,
  b.custompinyin,
  b.corpcode,
  b.customname,
  b.creditflag,
  b.credit,
  b.creditdaysflag,
  b.creditdays,
  b.tranpriority,
  b.defaulttranmethodid,
  b.defaultinvoicetype,
  b.fmid,
  c.fmopcode,
  c.fmname,
  c.fmrate,
  b.financeno,
  b.memo,
  b.credate,
  b.inputmanid,
  b.usestatus,
  b.recmoney,
  b.recdate,
  b.cityid,
  ci.cityname,
  ci.provinceid,
  ci.provincename,
  b.countryid,
  co.countryopcode,
  co.countryname,
  b.customertype,
  b.registadd,
  b.address,
  b.payway,
  '' AS salerid,
  nvl(b.reqprintquflag, 0) AS reqprintquflag,
  b.defaultpriceid,
  b.delivermethod,
  b.lowpriceflag,
  e.pricename,
  b.financeclass,
  e.opcode AS priceopcode,
  b.gspflag,
  b.customlevel,
  b.taxregisterno,
  b.hospitaladdress,
  b.unusualhome,
  b.hosbednumber,
  b.daypatients,
  b.ishealthcare,
  b.ismedicorg,
  b.isprofitmedicorg,
  NULL AS managerage,
  b.categoryid,
  f.ddlname AS financeclassname,
  g.ddlname AS customertypename,
  b.tob2bdateywy,
  b.zx_cusunitname,
  b.zx_maincustomid,
  b.zx_mainflag,
  b.legalperson,
  b.zx_socialcode
FROM ranked_data a
JOIN ods_erp.pub_customer b ON a.customid=b.customid AND a.DW_CREATETIME=b.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_formoney c ON b.fmid = c.fmid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_city ci ON b.cityid = ci.cityid AND ci.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_country co ON b.countryid = co.countryid AND co.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_price_type e ON b.defaultpriceid = e.priceid AND e.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_ddl_dtl f ON b.financeclass = f.ddlid AND f.sysid = 100047 AND f.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_ddl_dtl g ON b.customertype = g.ddlid AND g.sysid = 781 AND g.valid_to = DATE('9999-12-31')