MODEL (
  name sqlmesh_dim.erp_pub_entry_goods,
  kind FULL,
  dialect doris,
  grain (entryid, goodsid),
  physical_properties (
    unique_key = (valid_from, valid_to, entryid, goodsid),
    distributed_by = (kind='HASH', expressions=entryid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "独立单元货品"
);

WITH ranked_data AS (
    SELECT entryid, goodsid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY entryid, goodsid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    entryid, goodsid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_entry_goods
),
spzt_map AS (
  SELECT 1 AS optionid, '新品' AS status, 'Spzt_Xp=1' AS expr UNION ALL
  SELECT 2, '淘汰停产', 'Spzt_Ttxc=1' UNION ALL
  SELECT 3, '淘汰停采', 'Spzt_Tttc=1' UNION ALL
  SELECT 4, '淘汰替换', 'Spzt_Tttf=1' UNION ALL
  SELECT 5, '厂家控货', 'Spzt_Cjkh=1' UNION ALL
  SELECT 6, '常规品', 'Spzt_Cgp=1' UNION ALL
  SELECT 7, '门店跟销品', 'Spzt_Mdgxp=1' UNION ALL
  SELECT 8, '赠品', 'Spzt_Zp=1' UNION ALL
  SELECT 9, '在营品', 'Zx_Zyp=1' UNION ALL
  SELECT 10, '必备品种', 'spzt_bbpz=1' UNION ALL
  SELECT 11, '战略品种', 'spzt_zlpz=1' UNION ALL
  SELECT 12, '重点品种', 'spzt_zdpz=1' UNION ALL
  SELECT 13, '主推品种', 'spzt_ztpz>0' UNION ALL
  SELECT 14, '统采品', 'spzt_tcp=1' UNION ALL
  SELECT 15, '考核品种', 'spzt_khpz=1' UNION ALL
  SELECT 16, '专区品种', 'spzt_zqpz=1' UNION ALL
  SELECT 17, '电商TOP100品种', 'spzt_dstop=1' UNION ALL
  SELECT 18, '调拨品种', 'spzt_dbpz=1'
),
spzt_logic AS (
  SELECT
    a.entryid,
    a.goodsid,
    CASE WHEN a.Spzt_Xp = 1 THEN '新品' END AS spzt1,
    CASE WHEN a.Spzt_Ttxc = 1 THEN '淘汰停产' END AS spzt2,
    CASE WHEN a.Spzt_Tttc = 1 THEN '淘汰停采' END AS spzt3,
    CASE WHEN a.Spzt_Tttf = 1 THEN '淘汰替换' END AS spzt4,
    CASE WHEN a.Spzt_Cjkh = 1 THEN '厂家控货' END AS spzt5,
    CASE WHEN a.Spzt_Cgp = 1 THEN '常规品' END AS spzt6,
    CASE WHEN a.Spzt_Mdgxp = 1 THEN '门店跟销品' END AS spzt7,
    CASE WHEN a.Spzt_Zp = 1 THEN '赠品' END AS spzt8,
    CASE WHEN a.Zx_Zyp = 1 THEN '在营品' END AS spzt9,
    CASE WHEN a.Spzt_Bbpz = 1 THEN '必备品种' END AS spzt10,
    CASE WHEN a.Spzt_Zlpz = 1 THEN '战略品种' END AS spzt11,
    CASE WHEN a.Spzt_Zdpz = 1 THEN '重点品种' END AS spzt12,
    CASE WHEN a.Spzt_Ztpz > 0 THEN '主推品种' END AS spzt13,
    CASE WHEN a.Spzt_tcp = 1 THEN '统采品' END AS spzt14,
    CASE WHEN a.Spzt_khpz = 1 THEN '考核品种' END AS spzt15,
    CASE WHEN a.Spzt_zqpz = 1 THEN '专区品种' END AS spzt16,
    CASE WHEN a.Spzt_dstop = 1 THEN '电商TOP100品种' END AS spzt17,
    CASE WHEN a.Spzt_dbpz = 1 THEN '调拨品种' END AS spzt18
  FROM ods_erp.pub_entry_goods a
),
spzt AS (
  SELECT entryid, goodsid, GROUP_CONCAT(spzt) AS spzt
  FROM (
    SELECT entryid, goodsid, spzt1 AS spzt FROM spzt_logic WHERE spzt1 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt2 FROM spzt_logic WHERE spzt2 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt3 FROM spzt_logic WHERE spzt3 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt4 FROM spzt_logic WHERE spzt4 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt5 FROM spzt_logic WHERE spzt5 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt6 FROM spzt_logic WHERE spzt6 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt7 FROM spzt_logic WHERE spzt7 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt8 FROM spzt_logic WHERE spzt8 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt9 FROM spzt_logic WHERE spzt9 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt10 FROM spzt_logic WHERE spzt10 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt11 FROM spzt_logic WHERE spzt11 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt12 FROM spzt_logic WHERE spzt12 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt13 FROM spzt_logic WHERE spzt13 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt14 FROM spzt_logic WHERE spzt14 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt15 FROM spzt_logic WHERE spzt15 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt16 FROM spzt_logic WHERE spzt16 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt17 FROM spzt_logic WHERE spzt17 IS NOT NULL UNION ALL
    SELECT entryid, goodsid, spzt18 FROM spzt_logic WHERE spzt18 IS NOT NULL
  ) t
  GROUP BY entryid, goodsid
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
  -- Goods columns
  a.entryid,
  a.goodsid,
  g.opcode,
  g.goodspinyin,
  g.goodsname,
  g.currencyname,
  g.goodsengname,
  g.goodsinvname,
  g.goodsshortname,
  g.goodsformalname,
  g.goodsformalpy,
  g.goodsno,
  g.standardno,
  g.barcode,
  g.goodstype,
  g.goodsunit,
  g.approvedocno,
  g.registdocno,
  g.standardtype,
  g.factoryid,
  f.factoryopcode,
  f.factoryname,
  g.validperiod,
  g.respperiod,
  g.periodunit,
  g.trademark,
  g.prodarea,
  IFNULL(tax.supplytaxrate, g.supplytaxrate) AS supplytaxrate,
  IFNULL(tax.salestaxrate, g.salestaxrate) AS salestaxrate,
  g.fixpricetype,
  g.varietyid,
  v.opcode AS varietyopcode,
  v.varietyname,
  vd.varietydescid,
  vd.opcode AS varietydescopcode,
  vd.varietydescno,
  vd.varietydescname,
  g.accflag,
  g.defaultagtflag,
  g.storagecondition,
  g.transcondition,
  g.combinflag,
  g.customstax,
  IFNULL(a.minreqgoodsqty, g.minreqgoodsqty) AS minreqgoodsqty,
  g.financeno,
  -- Entrygoods columns
  a.otcflag,
  g.gmpflag,
  g.patentflag,
  g.securityflag,
  g.chineseflag,
  g.medicinetype,
  mt.ddlname AS medicinetypename,
  g.importflag,
  g.drugflag,
  g.poisonflag,
  g.biomedflag,
  g.bacterinflag,
  g.commonflag,
  g.medicineflag,
  g.credate,
  g.inputmanid,
  emp.employeename AS inputmanname,
  g.memo,
  a.usestatus AS wholestatus,
  v.varietyno,
  g.busiscope,
  bs.scopename AS busiscopename,
  g.boxflag,
  a.function,
  g.familyplanflag,
  g.againchkflag,
  vdc.vardesclassname,
  vdc.vardesclassid,
  g.medicinesort,
  g.limitedsaleflag,
  g.leastsaleqty,
  g.ephedrine,
  g.iseggpeptide,
  IFNULL(g.gspflag, 0) AS gspflag,
  IFNULL(g.ecodeflag, 0) AS ecodeflag,
  g.constituent,
  g.reqprintquflag,
  g.highcostflag,
  g.lowcostflag,
  g.importsplitflag,
  g.armariumflag,
  g.implantable,
  g.externalagentia,
  g.artificialorganflag,
  g.interventional,
  g.gmpinvaliddate,
  g.productapproval,
  g.zx_finance_class,
  g.hiddenflag,
  g.invaliddateflag,
  g.grade AS grades,
  g.dropingflag,
  IFNULL(a.usestatus, 0) AS usestatus,
  IFNULL(a.gspusestatus, 0) AS gspusestatus,
  IFNULL(a.bmsusestatus, 0) AS bmsusestatus,
  a.keyconserveflag,
  a.toinvdayswarn,
  a.firstsudate,
  a.firstsudays,
  a.phyconservedays,
  a.keysphyconservedays,
  a.sellattribute,
  a.isgovflag,
  a.strategyflag,
  a.goodsclass,
  a.printclass,
  a.goodsintegration,
  a.paymethod,
  a.paylimit,
  a.bidperiod,
  a.isbasemedic,
  a.ishealthcare,
  a.healthcareno,
  a.invoicename,
  a.entrymemo,
  a.discountuplimit,
  a.pricetagftype,
  a.parnflag,
  a.ptolevel,
  a.goodssutype,
  IFNULL(g.zx_policyno, a.policyno) AS policyno,
  a.policyname,
  a.ispolicy,
  a.invoiceitem,
  a.isprecious,
  a.getmoneylevel,
  a.integernx,
  a.bigpackqty,
  a.onlyprescription,
  a.zx_goodsattribute,
  a.courseoftreatment,
  a.usageanddosage,
  a.dtcstatus,
  IFNULL(a.dtcflag, 0) AS dtcflag,
  a.dtcmemo,
  '' AS ismodify,
  g.groupmanagetype,
  gprice.price,
  a.machinerywebflag,
  a.maxqtyrequest,
  a.saseason,
  a.grade,
  a.materialcode,
  a.storagetype,
  a.harseason,
  a.prescriptionflag,
  a.goodstypes,
  a.zx_quality,
  a.dldyjbywlb,
  a.entrygoodsid,
  a.useflag,
  a.levelnum,
  sp.spzt,
  a.spzt_cgp,
  a.spzt_cjkh,
  a.spzt_mdgxp,
  a.spzt_tttc,
  a.spzt_tttf,
  a.spzt_ttxc,
  a.spzt_xp,
  a.spzt_zp,
  a.zx_zyp,
  a.spzt_tcp,
  a.zx_lbbz,
  lbbzname.ddlname AS zx_lbbzname,
  g.lbfl,
  g.lbflname,
  a.stopmanid,
  a.stopdate,
  stopman.employeename AS stopmanname,
  a.dtp,
  a.zx_sfsm,
  g.udicode,
  dldyjbywlbname.ddlname AS dldyjbywlbname,
  g.ruleid,
  f.corpcode,
  bs.ZX_FINANCE_CLASS AS busizx_finance_class,
  g.relaname,
  g.credate AS zx_credate,
  g.inputmanid AS zx_inputmanid,
  emp.employeename AS zx_inputmanname,
  IFNULL(a.usestatus, 0) AS zx_usestatus,
  a.zx_zy,
  g.zx_gjypbwm,
  g.zx_ssbq,
  a.zx_scxkzh,
  a.product_invaliddate,
  a.gmp_invaliddate,
  g.zx_tracecode_prefix,
  a.zx_maxrsaqty,
  g.zx_specgoods_flag,
  a.zx_ybfl,
  not_null_or_empty(t101321.zx_jtid) AS jtzg_flag,
  ev.entryname,
  g.zx_retainqty,
  g.zx_maingoodsid,
  g.zx_zyypgg,
  g.zx_productdescr,
  g.zx_model,
  g.zx_productno,
  g.zx_productcode
FROM ranked_data r
JOIN ods_erp.pub_entry_goods a ON r.entryid=a.entryid AND r.goodsid=a.goodsid AND r.DW_CREATETIME=a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_goods g ON a.goodsid = g.goodsid AND g.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_factory f ON g.factoryid = f.factoryid AND f.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_goods_variety v ON g.varietyid = v.varietyid AND v.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_goods_variety_desc vd ON v.varietydescid = vd.varietydescid AND vd.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_goods_variety_desc_class vdc ON vd.vardesclassid = vdc.vardesclassid AND vdc.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee emp ON g.inputmanid = emp.employeeid AND emp.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_bms_busiscope_def bs ON g.busiscope = bs.scopedefid AND bs.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_goods_price gprice ON a.goodsid = gprice.goodsid AND gprice.valid_to = DATE('9999-12-31') AND gprice.priceid = 2
LEFT JOIN sqlmesh_dim.erp_pub_price_type price ON gprice.priceid = price.priceid AND price.valid_to = DATE('9999-12-31')
LEFT JOIN spzt sp ON a.entryid = sp.entryid AND a.goodsid = sp.goodsid
LEFT JOIN sqlmesh_dim.erp_pub_employee stopman ON a.stopmanid = stopman.employeeid AND stopman.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_ddl_dtl lbbzname ON a.zx_lbbz = lbbzname.ddlid AND lbbzname.sysid = 100786 AND lbbzname.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_ddl_dtl dldyjbywlbname ON a.dldyjbywlb = dldyjbywlbname.ddlid AND dldyjbywlbname.sysid = 100184 AND dldyjbywlbname.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_sys_ddl_dtl mt ON g.Medicinetype = mt.Ddlid AND mt.sysid = 15 AND mt.valid_to = DATE('9999-12-31')
LEFT JOIN (
  SELECT t.supplytaxrate, t.salestaxrate, t.entryid, t.goodsid
  FROM sqlmesh_dim.erp_zx_pub_taxrate t
  WHERE t.customid IS NULL AND t.supplyid IS NULL AND t.valid_to = DATE('9999-12-31')
) tax ON a.entryid = tax.entryid AND a.goodsid = tax.goodsid 
LEFT JOIN sqlmesh_dim.erp_t_101321 t101321 ON a.goodsid = t101321.goodsid AND t101321.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_entry ev ON a.entryid = ev.entryid AND ev.valid_to = DATE('9999-12-31')