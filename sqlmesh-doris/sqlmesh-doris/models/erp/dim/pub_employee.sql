MODEL (
  name sqlmesh_dim.erp_pub_employee,
  kind FULL,
  dialect doris,
  grain employeeid,
  physical_properties (
    unique_key = (valid_from, valid_to, employeeid),
    distributed_by = (kind='HASH', expressions=employeeid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "员工维度表"
);

WITH ranked_data AS (
    SELECT EMPLOYEEID, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY EMPLOYEEID
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    EMPLOYEEID
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_employee
),
ancestor_entry AS (
  SELECT
    a.employeeid,
    a.deptid,
    CASE
      WHEN c2.selfflag = 1 THEN c2.companyid
      WHEN c3.selfflag = 1 THEN c3.companyid
      WHEN c4.selfflag = 1 THEN c4.companyid
      ELSE NULL
    END AS ancestor_companyid
  FROM ods_erp.pub_employee a
  LEFT JOIN sqlmesh_dim.erp_pub_company c1 ON a.deptid = c1.companyid AND c1.valid_to = DATE('9999-12-31')
  LEFT JOIN sqlmesh_dim.erp_pub_company c2 ON c1.parentcompanyid = c2.companyid AND c2.valid_to = DATE('9999-12-31')
  LEFT JOIN sqlmesh_dim.erp_pub_company c3 ON c2.parentcompanyid = c3.companyid AND c3.valid_to = DATE('9999-12-31')
  LEFT JOIN sqlmesh_dim.erp_pub_company c4 ON c3.parentcompanyid = c4.companyid AND c4.valid_to = DATE('9999-12-31')
),
entryid_lookup AS (
  SELECT
    a.employeeid,
    MAX(e.entryid) AS entryid
  FROM ancestor_entry a
  LEFT JOIN sqlmesh_dim.erp_pub_entry e ON a.ancestor_companyid = e.entrycompanyid AND e.valid_to = DATE('9999-12-31')
  GROUP BY a.employeeid
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
  b.employeeid,
  b.opcode,
  b.employeename,
  b.pinyin,
  b.sex,
  b.birthdate,
  b.idcard,
  b.deptid,
  c1.companyopcode AS deptopcode,
  c1.companyname AS deptname,
  b.memo,
  b.usestatus,
  b.email,
  b.phone,
  b.mobileno,
  b.address,
  b.station,
  b.jobedu,
  b.specialty,
  b.edulevel,
  b.selfflag,
  b.financeno,
  b.webpass,
  eid.entryid,
  d.entryname,
  b.entrytime,
  b.leavejobdate,
  IFNULL(b.leavejobstatus,0) AS leavejobstatus,
  b.belong,
  b.zx_post,
  c1.parentcompanyid,
  b.checkflag,
  b.checkflag2
FROM ranked_data a
JOIN ods_erp.pub_employee b ON a.employeeid=b.employeeid AND a.DW_CREATETIME=b.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_company c1 ON b.deptid = c1.companyid AND c1.valid_to = DATE('9999-12-31')
LEFT JOIN entryid_lookup eid ON b.employeeid = eid.employeeid
LEFT JOIN sqlmesh_dim.erp_pub_entry d ON eid.entryid = d.entryid AND d.valid_to = DATE('9999-12-31')
WHERE b.employeeid > 0;
