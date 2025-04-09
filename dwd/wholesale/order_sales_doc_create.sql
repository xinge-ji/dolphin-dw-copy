DROP TABLE IF EXISTS dwd.wholesale_order_sales_doc;
CREATE TABLE dwd.wholesale_order_sales_doc (
    -- 主键标识
    salesid bigint COMMENT '销售单ID',
    dw_updatetime datetime COMMENT '数据更新时间',
    
    -- 时间维度
    create_date datetime COMMENT '创建日期',
    confirm_date datetime COMMENT '确认日期',
    
    -- 组织维度
    entryid bigint COMMENT '独立单元ID',
    entry_name varchar COMMENT '独立单元名称',
    province_name varchar COMMENT '省份名称',
    city_name varchar COMMENT '城市名称',
        
    -- 客户维度
    customid bigint COMMENT '客户ID',
    customer_name varchar COMMENT '客户名称',
    customertype_name varchar COMMENT '客户类型名称',
    customertype_group varchar COMMENT '客户类型分组',

    -- 订单状态
    use_status varchar COMMENT '使用状态:正式/临时',
    
    -- 来源相关
    comefrom varchar COMMENT '订单来源',
    pt_orderid decimal(38,18) COMMENT '海西订单号',
    is_haixi tinyint COMMENT '是否海西订单',
    cgdbh varchar COMMENT '药械网单号',
    is_yaoxiewang tinyint COMMENT '是否药械网订单',
    econid bigint COMMENT 'B2B平台订单ID（电商订单）',
    is_btob tinyint COMMENT '是否B2B订单',
    ordernum varchar COMMENT 'B2B平台系统订单ID（电商订单）',
    
    -- 销售模式
    peihuojiekuan_mode varchar COMMENT '销售模式:先配后结/先结后配',
    sale_type varchar COMMENT '销售类型:销售/销退',
    sale_mode varchar COMMENT '销售方式:普通销售/委托销售/寄售',
    
    -- 结算相关
    settletypeid bigint COMMENT '结算类型ID',
    settle_type varchar COMMENT '结算类型',
    credit_approve_status varchar COMMENT '审批状态',
    
    -- 金额
    sales_amount DECIMAL(18,4) COMMENT '销售金额',
    
    -- 人员维度
    salerid bigint COMMENT '销售员ID',
    saler_name varchar COMMENT '销售员名称',
    inputmanid bigint COMMENT '制单人ID',
    inputman_name varchar COMMENT '制单人名称'
)
UNIQUE KEY(salesid) DISTRIBUTED BY HASH(salesid) PROPERTIES (
  "replication_allocation" = "tag.location.default: 3",
  "in_memory" = "false",
  "storage_format" = "V2",
  "disable_auto_compaction" = "false"
);

-- 一次性插入所有数据，整合了原来的多个UPDATE操作
INSERT INTO dwd.wholesale_order_sales_doc (
    -- 主键标识
    salesid,
    dw_updatetime,
    
    -- 时间维度
    create_date,
    confirm_date,
    
    -- 组织维度
    entryid,
    entry_name,
    province_name,
    city_name,
        
    -- 客户维度
    customid,
    customer_name,
    customertype_name,
    customertype_group,

    -- 订单状态
    use_status,
    
    -- 来源相关
    comefrom,
    pt_orderid,
    is_haixi,
    cgdbh,
    is_yaoxiewang,
    econid,
    is_btob,
    ordernum,
    
    -- 销售模式
    peihuojiekuan_mode,
    sale_type,
    sale_mode,
    
    -- 结算相关
    settletypeid,
    settle_type,
    credit_approve_status,
    
    -- 金额
    sales_amount,
    
    -- 人员维度
    salerid,
    saler_name,
    inputmanid,
    inputman_name
)
SELECT
    -- 主键标识
	CAST(a.salesid as bigint),                  -- 销售单ID
    a.dw_updatetime,                            -- 数据更新时间
    
    -- 时间维度
    a.credate,                                  -- 创建日期
    a.confirmdate,                              -- 确认日期
    
    -- 组织维度
    a.entryid,                                  -- 独立单元ID
    e.entry_name,                               -- 独立单元名称
    e.province_name,                            -- 省份名称
    e.city_name,                                -- 城市名称
    
    -- 客户维度
    a.customid,                                 -- 客户ID
    c.customer_name,                            -- 客户名称
    c.customertype_name,                        -- 客户类型名称
    c.customertype_group,                       -- 客户类型分组
    
    -- 订单状态
    CASE 
    	WHEN a.usestatus = 1 THEN '正式'
        WHEN a.usestatus = 2 THEN '临时'
        ELSE 'UNKNOWN'
    END as use_status,                          -- 使用状态
    
    -- 来源相关
    b.ddlname as comefrom,                      -- 订单来源
    a.pt_orderid,                               -- 海西订单号
    CASE 
    	WHEN a.pt_orderid is not null THEN 1
        ELSE 0
    END AS is_haixi,                            -- 是否海西订单
    a.cgdbh,                                    -- 药械网单号
    CASE
    	WHEN a.cgdbh is not null THEN 1
        ELSE 0
    END as is_yaoxiewang,                       -- 是否药械网订单
    a.econid,                                   -- B2B平台订单ID
    CASE 
    	WHEN a.econid is not null then 1
        ELSE 0
    END AS is_btob,                             -- 是否B2B订单
    a.ordernum,                                 -- B2B平台系统订单ID
    
    -- 销售模式
    CASE
    	WHEN a.fix_mode = 1 THEN '先配后结'
        WHEN a.fix_mode = 2 THEN '先结后配'
        ELSE 'UNKNOWN'
    END AS peihuojiekuan_mode,                  -- 销售模式
    CASE
    	WHEN a.satypeid = 1 THEN '销售'
        WHEN a.satypeid = 2 THEN '销退'
        ELSE 'UNKNOWN'
    END as sale_type,                           -- 销售类型
    CASE
    	WHEN a.sa_mode = 1 THEN '普通销售'
        WHEN a.sa_mode = 2 THEN '委托销售'
        WHEN a.sa_mode = 3 THEN '寄售'
    	ELSE 'UNKNOWN'
    END as sale_mode,                           -- 销售方式
    
    -- 结算相关
    a.settletypeid,                             -- 结算类型ID
    st.settle_type,                             -- 结算类型
    CASE
        WHEN a.zx_creditapprovestatus = 1 THEN '初始状态'
        WHEN a.zx_creditapprovestatus = 2 THEN '待审批'
        WHEN a.zx_creditapprovestatus = 3 THEN '审批通过'
        WHEN a.zx_creditapprovestatus = 4 THEN '审批不通过'
        ELSE 'UNKNOWN'
    END AS credit_approve_status,               -- 审批状态
    
    -- 金额
    a.total,                                    -- 销售金额
    
    -- 人员维度
    a.salerid,                                  -- 销售员ID
    saler.employee_name,                        -- 销售员名称
    a.inputmanid,                               -- 制单人ID
    inputman.employee_name                      -- 制单人名称
FROM
	ods_erp.bms_sa_doc a                        -- 销售单主表
LEFT JOIN (
    SELECT
      ddlid,
      MIN(ddlname) as ddlname
    FROM
      ods_erp.sys_ddl_dtl
    WHERE
      sysid = 242
      AND is_active = 1
    GROUP BY
      ddlid
) b ON a.comefrom = b.ddlid                     -- 订单来源表
LEFT JOIN
    dim.entry e ON a.entryid = e.entryid
    AND a.credate >= e.dw_starttime AND a.credate < e.dw_endtime        -- 独立单元维度表
LEFT JOIN
    dim.customer c ON a.customid = c.customid 
    AND a.credate >= c.dw_starttime AND a.credate < c.dw_endtime  -- 客户维度表
LEFT JOIN
    dim.settle_type st ON a.settletypeid = st.settletypeid
    AND a.credate >= st.dw_starttime AND a.credate < st.dw_endtime   -- 结算类型维度表
LEFT JOIN
    dim.employee saler ON a.salerid = saler.employeeid
    AND a.credate >= saler.dw_starttime AND a.credate < saler.dw_endtime       -- 销售员维度表
LEFT JOIN
    dim.employee inputman ON a.inputmanid = inputman.employeeid
    AND a.credate >= inputman.dw_starttime AND a.credate < inputman.dw_endtime   -- 制单人维度表
WHERE 
    a.is_active = 1;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_sales_doc_entryid ON dwd.wholesale_order_sales_doc (entryid);
CREATE INDEX IF NOT EXISTS idx_sales_doc_customid ON dwd.wholesale_order_sales_doc (customid);
CREATE INDEX IF NOT EXISTS idx_sales_doc_create_date ON dwd.wholesale_order_sales_doc (create_date);