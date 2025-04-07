TRUNCATE TABLE dwd.wholesale_customer_ar_dtl;

-- 来源 ERP: v_lsqk
INSERT INTO dwd.wholesale_customer_ar_dtl (
    customid,
    entryid,
    sourcetable,
    sourceid,
    salerid,
    bill_date,
    bill_amount
)
SELECT 
    -- 客户和组织维度
    c.customid,                 -- 客户ID
    c.entryid,                  -- 独立单元ID

    -- 单据信息
    CASE
        WHEN h.oldsourceid IS NOT NULL THEN h.sourcetable
        ELSE c.sourcetable
    END AS sourcetable,         -- 来源表名
    
    CASE
        WHEN h.oldsourceid IS NOT NULL THEN h.sourceid
        ELSE c.sourceid
    END AS sourceid,            -- 来源单据ID

    -- 人员维度
    c.salerid,                  -- 销售员ID
    
    CASE
        WHEN h.oldsourceid IS NOT NULL THEN h.billdate
        ELSE c.billdate
    END AS bill_date,            -- 单据日期
    
    CASE
        WHEN h.oldsourceid IS NOT NULL THEN h.billmoney
        WHEN j.sa_mode IS NOT NULL THEN 0  -- 委托销售没有结算不算欠款
        ELSE c.billmoney
    END AS bill_amount           -- 欠款金额
FROM 
    ods_erp.bms_credit_bill_tmp c
    
    -- 左连接：委托销售/寄售的结算单信息
    LEFT JOIN (
        SELECT 
            a1.salesid AS oldsourceid,
            'BMS_SA_DOC' AS oldesourcetable,
            'BMS_SA_SETTLE_DTL' AS sourcetable,
            e.sasettledtlid AS sourceid,
            f.credate AS billdate,
            (IFNULL(e.total_line, 0) - IFNULL(e.totalrecmoney, 0)) AS billmoney
        FROM 
            ods_erp.bms_sa_doc a1
            JOIN ods_erp.bms_sa_dtl c1 ON a1.salesid = c1.salesid
            JOIN ods_erp.bms_sa_doctoset d ON c1.salesdtlid = d.salesdtlid
            JOIN ods_erp.bms_sa_settle_dtl e ON d.sasettledtlid = e.sasettledtlid
            JOIN ods_erp.bms_sa_settle_doc f ON e.sasettleid = f.sasettleid
        WHERE 
            a1.sa_mode IN (2, 3)                 -- 2:委托销售, 3:寄售
            AND f.usestatus IN (1, 2)            -- 1:正式, 2:临时
            AND IFNULL(e.recfinflag, 0) <> 2     -- 排除不收款单子
            AND (IFNULL(e.total_line, 0) - IFNULL(e.totalrecmoney, 0)) <> 0
            AND a1.is_active = 1
            AND c1.is_active = 1
            AND d.is_active = 1
            AND e.is_active = 1
    ) h ON c.sourceid = h.oldsourceid AND c.sourcetable = h.oldesourcetable
    
    -- 左连接：识别委托销售/寄售的销售单
    LEFT JOIN (
        SELECT 
            i.sa_mode, 
            i.salesid, 
            'BMS_SA_DOC' AS sourcetable
        FROM 
            ods_erp.bms_sa_doc i
        WHERE 
            i.sa_mode IN (2, 3)                  -- 2:委托销售, 3:寄售
            AND i.is_active = 1
    ) j ON c.sourceid = j.salesid AND c.sourcetable = j.sourcetable
WHERE 
    -- 排除已经审批通过且在有效期内的信用账单
    NOT EXISTS (
        SELECT 1
        FROM ods_erp.t_101229_doc a
        JOIN ods_erp.t_101229_dtl b ON a.docid = b.docid
        WHERE 
            a.usestatus = 2                      -- 审批通过
            AND b.sourceid = c.sourceid
            AND b.sourcetable = c.sourcetable
            AND a.expiry_date > CURRENT_DATE()   -- 在有效期内
    )
    -- 只处理销售单和结算单
    AND c.sourcetable IN ('BMS_SA_DOC', 'BMS_SA_SETTLE_DTL')
    AND c.is_active = 1;  