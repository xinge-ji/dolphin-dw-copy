DROP TABLE IF EXISTS dim.employee;

CREATE TABLE
    dim.employee (
        employeeid bigint COMMENT '员工ID',
        dw_starttime datetime COMMENT '数据生效时间',
        dw_endtime datetime COMMENT '数据失效时间',
        is_active tinyint COMMENT '是否有效',
        employee_name varchar COMMENT '员工姓名',
        use_status tinyint COMMENT '使用状态'
    ) UNIQUE KEY (employeeid, dw_starttime) DISTRIBUTED BY HASH (employeeid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO
    dim.employee (
        employeeid,
        dw_starttime,
        dw_endtime,
        is_active,
        employee_name,
        use_status
    )
WITH
    ranked_employee AS (
        SELECT
            employeeid,
            dw_createtime,
            dw_updatetime,
            is_active,
            employeename,
            usestatus,
            ROW_NUMBER() OVER (
                PARTITION BY
                    employeeid
                ORDER BY
                    dw_createtime
            ) AS record_seq,
            LEAD (date (dw_createtime), 1, NULL) OVER (
                PARTITION BY
                    employeeid
                ORDER BY
                    dw_createtime
            ) AS next_start_time
        FROM
            ods_erp.pub_employee
    )
SELECT
    employeeid,
    CASE
      WHEN record_seq = 1 THEN LEAST(date('1970-01-01'), date(dw_createtime))
      ELSE date(dw_createtime)
    END AS dw_starttime,
    CASE
      WHEN next_start_time IS NOT NULL THEN next_start_time
      WHEN dw_createtime <> dw_updatetime THEN date(dw_updatetime)
      ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
    END AS dw_endtime,
    CASE
      WHEN next_start_time IS NOT NULL THEN 0
      WHEN dw_createtime <> dw_updatetime THEN 0
      ELSE 1
    END AS is_active,
    employeename,
    usestatus
FROM
    ranked_employee;


CREATE INDEX IF NOT EXISTS idx_startdates ON dim.employee (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.employee (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.employee (is_active);