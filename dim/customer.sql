DROP TABLE IF EXISTS dim.customer;

CREATE TABLE
    dim.customer (
        customid bigint comment '客户id',
        dw_starttime datetime comment '数据开始时间',
        dw_endtime datetime comment '数据结束时间',
        is_active tinyint comment '是否活跃',
        create_date datetime comment '创建时间',
        customer_name varchar comment '客户名称',
        financeclass int comment '财务类别',
        customer_financeclass_name varchar comment '财务类别名称',
        customertype int comment '客户类型',
        customertype_name varchar comment '客户类型名称',
        is_shuangwanjia tinyint comment '是否双万家',
        customertype_group varchar comment '双控客户类型:企业/公立等级/公立基层/私营等级/私营基层',
        customertype_task varchar comment '分销项目客户类型:等级机构/基层/终端/企业'
    ) UNIQUE KEY (customid, dw_starttime) DISTRIBUTED BY HASH (customid) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

INSERT INTO
    dim.customer (
        customid,
        dw_starttime,
        dw_endtime,
        is_active,
        create_date,
        customer_name,
        financeclass,
        customer_financeclass_name,
        customertype,
        customertype_name,
        is_shuangwanjia,
        customertype_group,
        customertype_task
    )
WITH
    ranked_customer AS (
        SELECT
            customid,
            dw_createtime,
            dw_updatetime,
            is_active,
            credate,
            customname,
            financeclass,
            customertype,
            zx_strategicunit,
            ROW_NUMBER() OVER (
                PARTITION BY
                    customid
                ORDER BY
                    dw_createtime
            ) AS record_seq,
            LEAD (date (dw_createtime), 1, NULL) OVER (
                PARTITION BY
                    customid
                ORDER BY
                    dw_createtime
            ) AS next_start_time
        FROM
            ods_erp.pub_customer
    )
SELECT
    t1.customid,
    CASE
      WHEN t1.record_seq = 1 THEN LEAST(date('1970-01-01'), date(t1.dw_createtime))
      ELSE date(t1.dw_createtime)
    END AS dw_starttime,
    CASE
      WHEN t1.next_start_time IS NOT NULL THEN t1.next_start_time
      WHEN t1.dw_createtime <> t1.dw_updatetime THEN date(t1.dw_updatetime)
      ELSE CAST('9999-12-31 23:59:59' AS DATETIME)
    END AS dw_endtime,
    CASE
      WHEN t1.next_start_time IS NOT NULL THEN 0
      WHEN t1.dw_createtime <> t1.dw_updatetime THEN 0
      ELSE 1
    END AS is_active,
    t1.credate,
    t1.customname as customer_name,
    CAST(t1.financeclass AS INT),
    t2.ddlname as customer_financeclass_name,
    CAST(t1.customertype AS INT),
    t3.ddlname as customertype_name,
    CAST(IFNULL (t1.zx_strategicunit, 0) AS TINYINT) as is_shuangwanjia,
    CASE
        WHEN t1.customertype IN (1, 2, 3, 4, 5) THEN '企业'
        WHEN t1.customertype IN (6, 7, 8, 19) THEN '公立等级'
        WHEN t1.customertype IN (9, 10, 11, 12, 13, 20, 21, 22, 23, 24, 34) THEN '公立基层'
        WHEN t1.customertype IN (14, 15, 16, 25, 32, 38) THEN '私营等级'
        WHEN t1.customertype IN (17, 18, 26, 27, 28, 33, 35, 36) THEN '私营基层'
        ELSE '其他'
    END AS customertype_group,
    CASE
        WHEN t1.customertype IN (7, 8, 15, 16, 32, 38) THEN '等级机构' -- 二级及以上医疗机构（含公立及民营）
        WHEN t1.customertype IN (6, 9, 10, 11, 12, 14, 17, 18, 19, 20, 21, 22, 24, 25, 26, 28, 33, 34, 
                                 35, 36, 37) THEN '基层' -- 卫生院、社区卫生服务中心
        WHEN t1.customertype IN (13, 17, 23, 27, 31) THEN '终端' -- 村所、诊所、门诊部等
        WHEN t1.customertype IN (1, 2, 3, 4, 5, 29, 30) THEN '企业'
    END AS customertype_task
FROM
    ranked_customer t1
    LEFT JOIN (
        Select
            Ddlid,
            MIN(Ddlname) as ddlname
        From
            ods_erp.sys_ddl_dtl
        Where
            sysid = 100047
            and is_active = 1
        group by
            ddlid
    ) t2 ON t1.financeclass = t2.ddlid
    LEFT JOIN (
        Select
            Ddlid,
            MIN(Ddlname) as ddlname
        From
            ods_erp.sys_ddl_dtl
        Where
            sysid = 781
            and is_active = 1
        group by
            ddlid
    ) t3 ON t1.customertype = t3.ddlid;

CREATE INDEX IF NOT EXISTS idx_startdates ON dim.customer (dw_starttime);
CREATE INDEX IF NOT EXISTS idx_enddates ON dim.customer (dw_endtime);
CREATE INDEX IF NOT EXISTS idx_active ON dim.customer (is_active);

