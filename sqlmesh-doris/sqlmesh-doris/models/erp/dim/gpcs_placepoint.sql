MODEL (
  name sqlmesh_dim.erp_gpcs_placepoint,
  kind FULL,
  dialect doris,
  grain placepointid,
  physical_properties (
    unique_key = (valid_from, valid_to, placepointid),
    distributed_by = (kind='HASH', expressions=placepointid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "门店维度表",
  column_descriptions (
    exampriceid = "考核价ID",
    pricename = "考核价类型",
    examPriceOPcode = "考核价类型编码",
    prelotmanagermode = "药方是否精确管理到批号",
    remote_examine_flag = "远程审方",
    ecodeflag = "电子码标志",
    stdflag = "双通道标志",
    zx_scjxc_flag = "上传进销存标志",
    zx_scjxc_ybid = "上传进销存的医保ID",
    zx_checkmjqtyflag = "含麻碱数量验证",
    taxregisternum = "税号",
    kpcode = "极速开票代码",
    ylzypt = "易联众云平台标志",
    invtype = "发票类型",
    dtdflag = "单体店标志",
    hxbmbh = "航线部门编码"
  )
);

WITH ranked_data AS (
    SELECT placepointid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY placepointid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    placepointid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.gpcs_placepoint
)
SELECT
  CASE
    WHEN a1.record_seq = 1 
    THEN DATE('1970-01-01')
    ELSE date(a.DW_CREATETIME)
  END AS valid_from,
  CASE
    WHEN a1.next_start_time IS NOT NULL THEN a1.next_start_time
    ELSE DATE('9999-12-31')
  END AS valid_to,
  a.placepointid,
  a.placepointopcode,
  a.placepointno,
  a.placepointname,
  a.placepointpinyin,
  a.usestatus,
  a.type,
  a.memo,
  a.tel,
  a.ones,
  a.businesstime,
  a.address,
  a.credate,
  a.riserate,
  a.resa_money_precision,
  a.lot_manager_mode,
  a.lot_modify,
  a.maxsalesqty,
  a.updatepos,
  a.inputmethod,
  a.salerflag,
  a.workingtype,
  a.ctrllowprice,
  a.dispaygathmoney,
  a.storageid,
  b.opcode AS storageopcode,
  b.storagename,
  b.storageno,
  a.sthouseid,
  c.sthouseno,
  c.sthousename,
  a.inputmanid,
  d.employeename,
  d.opcode AS employeeopcode,
  a.areadocid,
  e.areaname,
  e.areaopcode,
  a.retailcenterid,
  f.retailcenteropcode,
  f.retailcentername,
  a.reqsplitflag,
  a.entryid,
  g.entryname,
  a.presstockflag,
  a.printflag,
  a.iscomm,
  a.reqcenterst,
  a.reqqtyuplimit,
  a.miniaturetaxpayerflag,
  a.incometaxrate,
  a.outputtaxrate,
  a.manualratelimit,
  IFNULL(a.ctrlmarageflag, 0) AS ctrlmarageflag,
  a.managerage,
  a.placepointtype,
  a.batch_manager_mode,
  a.otcrestrict,
  a.defaultdocter,
  a.defaulthospital,
  a.maxephqty,
  a.exampriceid,
  p.pricename,
  p.opcode AS examPriceOPcode,
  a.prelotmanagermode,
  a.svcardreceipttype,
  a.peifangmanid,
  a.shenfangmanid,
  pf.employeename AS peifangmanname,
  pf.opcode AS peifangmanopcode,
  sf.employeename AS shenfangmanname,
  sf.opcode AS shenfangmanopcode,
  a.dtcflag,
  a.remote_examine_flag,
  a.ecodeflag,
  a.stdflag,
  a.zx_scjxc_flag,
  a.camerano,
  a.zx_scjxc_ybid,
  a.maxsalesqty AS maxsaqty,
  a.zx_qhyybt,
  a.zx_checkmjqtyflag,
  a.taxregisternum,
  a.zx_xgm,
  a.kpcode,
  a.ylzypt,
  a.invtype,
  a.dtdflag,
  a.hxbmbh,
  a.reqsupplymanid,
  re.employeename AS reqsupplymanname,
  a.zx_specgoods_maxqty
FROM ranked_data a1
JOIN ods_erp.gpcs_placepoint a ON a1.placepointid=a.placepointid AND a1.DW_CREATETIME=a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_bms_st_def b ON a.storageid = b.storageid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_bms_st_storehouse c ON a.sthouseid = c.sthouseid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee d ON a.inputmanid = d.employeeid AND d.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_gpcs_shop_area_doc e ON a.areadocid = e.areadocid AND e.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_resa_retailcenter f ON a.retailcenterid = f.retailcenterid AND f.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_entry g ON a.entryid = g.entryid AND g.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_price_type p ON a.exampriceid = p.priceid AND p.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee pf ON a.peifangmanid = pf.employeeid AND pf.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee sf ON a.shenfangmanid = sf.employeeid AND sf.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee re ON a.reqsupplymanid = re.employeeid AND re.valid_to = DATE('9999-12-31'); 