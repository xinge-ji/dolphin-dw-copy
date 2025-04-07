INSERT INTO dwd.wholesale_order_settle_doc (sasettleid, __DORIS_DELETE_SIGN__)
SELECT sasettleid, 1
FROM ods_erp.bms_sa_settle_doc AS a
JOIN dwd.wholesale_order_settle_doc AS b 
ON a.sasettleid = b.sasettleid 
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;

INSERT INTO dwd.wholesale_order_settle_doc (
    sasettleid,
    dw_updatetime,
    create_date,
    confirm_date,
    use_status,
    entryid,
    customid,
    inputmanid,
    inputman_name
)
SELECT
    a.sasettleid,                               -- 订单结算单id
    a.dw_updatetime,                            -- 更新时间
    a.credate,                                  -- 创建时间
    a.confirmdate,                              -- 确认时间
    CASE
    	WHEN a.usestatus=0 THEN '作废'
        WHEN a.usestatus=1 THEN '正式'
        WHEN a.usestatus=2 THEN '临时'
        ELSE 'UNKNOWN'
    END AS use_status,                          -- 使用状态
    a.entryid,                                  -- 独立单元id
    a.customid,                                 -- 客户id
    a.inputmanid,                               -- 制单人id
    e.employee_name                             -- 制单人名称
FROM 
    ods_erp.bms_sa_settle_doc a                 -- 结算单主表
LEFT JOIN 
    dim.employee e ON a.inputmanid = e.employeeid
    AND a.credate >= e.dw_starttime AND a.credate < e.dw_endtime  -- 员工维度表
WHERE 
    a.is_active=1 AND dw_updatetime >= (
    SELECT
      max(dw_updatetime) - INTERVAL 60 DAY
    from
      dwd.wholesale_order_settle_doc
  );
