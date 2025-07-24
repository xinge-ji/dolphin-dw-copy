MODEL (
  name sqlmesh_dim.erp_gresa_prescription,
  kind FULL,
  dialect doris,
  grain (prescriptionid),
  physical_properties (
    unique_key = (valid_from, valid_to, prescriptionid),
    distributed_by = (kind='HASH', expressions=prescriptionid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "处方维度表"
);

WITH ranked_data AS (
    SELECT prescriptionid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY prescriptionid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    prescriptionid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.gresa_prescription
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
  a.prescriptionid,
  a.medicalandhealthorganizations,
  a.patient,
  a.diagnosis,
  a.credate,
  a.prescriptionnumber,
  a.doctor,
  a.inputmanid,
  a.companyid,
  b.employeename AS inputmanname,
  c.companyname,
  a.name,
  a.sex,
  a.birthday,
  a.age,
  a.phone,
  a.address,
  a.clinicaldiagnosis,
  a.peifangmanid,
  a.shenfangmanid,
  a.section,
  d.employeename AS peifangmanname,
  e.employeename AS shenfangmanname,
  d.opcode AS peifangmancode,
  e.opcode AS shenfangmancode,
  a.startdate,
  a.enddate,
  a.department,
  a.filegroupid,
  a.usestatus,
  a.idcode,
  a.nation,
  a.shenfangmemo,
  CASE WHEN a.filegroupid IS NULL OR a.filegroupid = 0 THEN 0 ELSE 1 END AS isfilegroupid,
  a.fuhemanid,
  f.employeename AS fuheman,
  a.yfyl,
  DATE_FORMAT(a.credate, '%Y-%m-%d') AS tpsj,
  IFNULL(DATE_FORMAT(a.sssj, '%Y-%m-%d'), DATE_FORMAT(a.credate, '%Y-%m-%d')) AS sssj,
  IFNULL(DATE_FORMAT(a.sfsj, '%Y-%m-%d'), DATE_FORMAT(a.credate, '%Y-%m-%d')) AS sfsj,
  a.keshi
FROM ranked_data r
JOIN ods_erp.gresa_prescription a ON r.prescriptionid = a.prescriptionid AND r.DW_CREATETIME = a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_employee b ON a.inputmanid = b.employeeid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_company c ON a.companyid = c.companyid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee d ON a.peifangmanid = d.employeeid AND d.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee e ON a.shenfangmanid = e.employeeid AND e.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_employee f ON a.fuhemanid = f.employeeid AND f.valid_to = DATE('9999-12-31'); 