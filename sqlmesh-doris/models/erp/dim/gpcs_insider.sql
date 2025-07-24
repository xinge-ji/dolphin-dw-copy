MODEL (
  name sqlmesh_dim.erp_gpcs_insider,
  kind FULL,
  dialect doris,
  grain insiderid,
  physical_properties (
    unique_key = (valid_from, valid_to, insiderid),
    distributed_by = (kind='HASH', expressions=insiderid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "会员维度表"
);

WITH ranked_data AS (
    SELECT insiderid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY insiderid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    insiderid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.gpcs_insider
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
  a.insiderid,
  a.insidername,
  a.opcode,
  a.openid,
  a.sex,
  a.birthdate,
  IFNULL(a.idcard, '000000000000000000') AS idcard,
  a.zipcode,
  a.homephone,
  a.officephone,
  a.mailaddress,
  a.emailaddress,
  a.rsacustomerno,
  a.inscompanyid,
  c.companyopcode AS incompanyopcode,
  c.companyname AS inscompanyname,
  a.hospitalid,
  d.companyopcode AS hosopcode,
  d.companyname AS hospitalname,
  a.socialsno,
  a.insuranceno,
  a.insurancecard,
  a.placepointid,
  f.placepointopcode,
  f.placepointname,
  a.inscardtypeid,
  e.cardopcode,
  e.cardtypename,
  a.renewal,
  a.bloodtype,
  a.medicard,
  a.elder,
  a.chlld,
  a.relationpeople,
  a.cardiopathy,
  a.hypertension,
  a.diabetes,
  a.boneill,
  a.highblood,
  a.bloodill,
  a.liverill,
  a.stomacthicill,
  a.womenfolkill,
  a.niaoill,
  a.aspiratoryill,
  a.otherill,
  a.memo,
  a.city,
  a.credate,
  a.usestatus,
  a.inputmanid,
  b.employeename,
  a.insidercardno,
  a.total,
  a.invaliddate,
  IFNULL(a.expressflag, 0) AS expressflag,
  IFNULL(a.integral, 0) AS integral,
  IFNULL(a.addintegral, 0) AS addintegral,
  IFNULL(a.addmoney, 0) AS addmoney,
  IFNULL(a.initintegral, 0) AS initintegral,
  IFNULL(a.initmoney, 0) AS initmoney,
  a.mobile,
  a.lastupgdate,
  a.firstmoney,
  a.cardtype,
  a.lunarbirthday,
  a.dealmanid,
  a.provinceid,
  a.cityid,
  a.countryid,
  a.lastconsumdate,
  a.lastresaid,
  a.reqservice,
  a.arrivetime,
  a.medicitype,
  a.spendpattern,
  a.pointimprove,
  a.recmessagetype,
  a.concerninfo,
  e1.opcode AS dealmanopcode,
  e1.employeename AS dealman,
  s1.provincename,
  a.inserlevel,
  s1.cityname,
  s2.countryopcode,
  s2.countryname,
  a.customid,
  h.customopcode,
  h.customname,
  a.weight,
  TIMESTAMPDIFF(YEAR, a.birthdate, NOW()) AS age,
  f.entryid,
  g.entrycompanyid,
  g.entrycompanyname
FROM ranked_data a1
JOIN ods_erp.gpcs_insider a ON a1.insiderid=a.insiderid AND a1.DW_CREATETIME=a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_employee b ON a.inputmanid = b.employeeid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_company c ON a.inscompanyid = c.companyid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_company d ON a.hospitalid = d.companyid AND d.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_gpcs_placepoint f ON a.placepointid = f.placepointid AND f.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_gpcs_insider_cardtype e ON a.inscardtypeid = e.inscardtypeid AND e.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee e1 ON a.dealmanid = e1.employeeid AND e1.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_city s1 ON a.cityid = s1.cityid AND s1.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_country s2 ON a.countryid = s2.countryid AND s2.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_customer h ON a.customid = h.customid AND h.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_entry g ON f.entryid = g.entryid AND g.valid_to = DATE('9999-12-31');