MODEL (
  name sqlmesh_dim.erp_pub_bank,
  kind FULL,
  dialect doris,
  grain (bankid),
  physical_properties (
    unique_key = (valid_from, valid_to, bankid),
    distributed_by = (kind='HASH', expressions=bankid, buckets=1),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "银行维度表",
  column_descriptions (
    provinceid = "省ID",
    cityid = "市ID", 
    countryid = "县ID"
  )
);

WITH ranked_data AS (
    SELECT bankid, DW_CREATETIME,
    ROW_NUMBER() OVER (
        PARTITION BY bankid
        ORDER BY
            DW_CREATETIME
      ) AS record_seq,
      LEAD (date (DW_CREATETIME), 1, NULL) OVER (
                PARTITION BY
                    bankid
                ORDER BY
                    DW_CREATETIME
            ) AS next_start_time
    FROM
      ods_erp.pub_bank
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
  a.bankid,
  a.bankopcode,
  a.bankno,
  a.bankpinyin,
  a.corpcode,
  a.bankname,
  a.bankmemo,
  a.usestatus,
  a.credate,
  a.inputmanid,
  b.employeename AS inputmanname,
  a.accno,
  a.provinceid,
  a.cityid,
  d.cityname,
  a.countryid,
  f.countryname
FROM ranked_data r
JOIN ods_erp.pub_bank a ON r.bankid = a.bankid AND r.DW_CREATETIME = a.DW_CREATETIME
LEFT JOIN sqlmesh_dim.erp_pub_employee b ON a.inputmanid = b.employeeid AND b.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_province c ON a.provinceid = c.provinceid AND c.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_city d ON a.cityid = d.cityid AND d.valid_to = DATE('9999-12-31')
LEFT JOIN sqlmesh_dim.erp_pub_country f ON a.countryid = f.countryid AND f.valid_to = DATE('9999-12-31'); 