DROP TABLE IF EXISTS dwd.wholesale_jicai_volume_dtl;

-- 来自 ERP V_LSQK_DL
CREATE TABLE dwd.wholesale_jicai_volume_dtl (
    -- 基础维度信息
    entryid bigint COMMENT '独立单元ID',
    customid bigint COMMENT '客户ID',
    salesdtlid bigint COMMENT '销售单ID',
    goodsid bigint COMMENT '商品ID',
    
    -- 时间信息
    create_date datetime COMMENT '创建日期',
    
    -- 集采相关信息
    docid bigint COMMENT '集采带量单据ID',
    dtlid bigint COMMENT '集采带量明细ID',
    jicai_batch_name varchar COMMENT '带量名称批次',
    dlbegindate datetime COMMENT '带量开始日期',
    dlenddate datetime COMMENT '带量结束日期',
    reported_qty decimal(16,6) COMMENT '报量数量',
    
    -- 回款相关
    contract_payment_days int COMMENT '协议回款天数',
    total_goods_qty decimal(16,6) COMMENT '累计销售数量',

    excess_payment_days int COMMENT '超量回款天数',
    
    -- 量内量外标识
    jicai_liangneiliangwai varchar(10) COMMENT '量内/量外标识',
    guankong_type int COMMENT '管控类型'
)
UNIQUE KEY(entryid, customid, salesdtlid, goodsid)
DISTRIBUTED BY HASH(customid, salesdtlid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2"
);

-- =====================================================
-- 插入集采带量欠款数据
-- =====================================================
INSERT INTO dwd.wholesale_jicai_volume_dtl (
    entryid,
    customid,
    salesdtlid,
    goodsid,
    create_date,
    docid,
    jicai_batch_name,
    dlbegindate,
    dlenddate,
    contract_payment_days,
    total_goods_qty,
    dtlid,
    reported_qty,
    excess_payment_days,
    jicai_liangneiliangwai,
    guankong_type
)
SELECT 
    -- 基础维度信息
    k.entryid,                  -- 独立单元ID
    k.customid,                 -- 客户ID
    k.salesdtlid,               -- 销售单ID
    k.goodsid,                  -- 商品ID
    
    -- 时间信息
    k.credate AS create_date,   -- 创建日期
    
    -- 集采相关信息
    k.docid,                    -- 集采带量单据ID
    k.dlmcpc AS jicai_batch_name, -- 带量名称批次
    k.dlbegindate,              -- 带量开始日期
    k.dlenddate,                -- 带量结束日期
    
    -- 回款相关
    CASE
        WHEN k.gklx = 2 THEN k.xyhkts                  -- 管控类型为2时，使用协议回款天数
        ELSE k.hkkdy * 30 - 15 + k.pdrq                -- 否则根据回款跨度月和判断日期计算
    END AS contract_payment_days,                      -- 协议回款天数
    
    k.ljxssl AS total_goods_qty, -- 累计销售数量
    k.dtlid,                    -- 带量明细ID
    CAST(k.blsl as decimal(16,6))  AS reported_qty,     -- 报量数量
    
    CASE
        WHEN k.dtlgklx = 2 THEN k.clhkts               -- 明细管控类型为2时，使用超量回款天数
        ELSE k.dtlhkkdy * 30 - 15 + k.dtlpdrq          -- 否则根据明细回款跨度月和判断日期计算
    END AS excess_payment_days,                        -- 超量回款天数
    
    -- 量内量外标识
    k.flag AS jicai_liangneiliangwai, -- 量内/量外标识
    k.gklx AS guankong_type    -- 管控类型
FROM (
    -- 子查询保持不变
    SELECT 
        g.*,                    -- 带量总单相关信息
        h.dtlid,                -- 明细ID
        h.blsl,                 -- 报量数量
        h.clhkts,               -- 超量回款天数
        h.pdrq AS dtlpdrq,      -- 量外判断日期
        h.hkkdy AS dtlhkkdy,    -- 量外回款跨度月
        h.gklx AS dtlgklx,      -- 量外管控类型
        
        -- 判断量内量外
        CASE
            -- 报量数量大于0，且累计销售数量大于报量数量，则判断为量外
            WHEN h.blsl > 0 AND g.ljxssl > h.blsl THEN '量外'
            ELSE '量内'
        END AS flag,
        
        -- 计算报量金额（优先取调整报量数量）
        IFNULL(IFNULL(h.tzblsl, h.blsl) * g.jczbj, 0) AS blje
    FROM (
        -- 子查询：获取带量超期欠款基础数据
        SELECT 
            d.entryid,          -- 独立单元ID
            d.customid,         -- 客户ID
            e.salesdtlid,       -- 销售单ID
            e.goodsid,          -- 商品ID
            d.credate,          -- 创建日期
            
            f.docid,            -- 集采带量单据ID
            f.jczbj,            -- 集采中标价
            f.dlmcpc,           -- 带量名称批次
            f.dlbegindate,      -- 带量开始日期
            f.dlenddate,        -- 带量结束日期
            f.xyhkts,           -- 协议回款天数
            
            -- 使用累计销售数量表替代子查询
            IFNULL(cs.cumulative_goods_qty, 0) AS ljxssl,
            
            f.gklx,             -- 管控类型
            f.pdrq,             -- 量内判断日期
            f.hkkdy             -- 量内回款跨度月
        FROM 
            ods_erp.bms_sa_doc d
        JOIN 
            ods_erp.bms_sa_dtl e ON d.salesid = e.salesid
        JOIN 
            ods_erp.t_101248_doc f ON d.entryid = f.entryid AND e.goodsid = f.goodsid
        LEFT JOIN 
            dwd.wholesale_jicai_cumulative_goods_qty cs ON d.entryid = cs.entryid 
                                                       AND e.goodsid = cs.goodsid 
                                                       AND d.credate = cs.create_date
        WHERE 
            IFNULL(e.total_line, 0) - IFNULL(e.totalrecmoney, 0) <> 0
            AND d.credate BETWEEN f.dlbegindate AND f.dlenddate
            AND f.usestatus = 1
            AND d.is_active = 1
            AND e.is_active = 1
            AND f.is_active = 1
    ) g
    LEFT JOIN 
        ods_erp.t_101248_dtl h ON g.docid = h.docid AND g.customid = h.customid AND h.is_active = 1
) k;


CREATE INDEX IF NOT EXISTS idx_entryid ON dwd.wholesale_jicai_volume_dtl (entryid);
CREATE INDEX IF NOT EXISTS idx_customid ON dwd.wholesale_jicai_volume_dtl (customid);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dwd.wholesale_jicai_volume_dtl (goodsid);
CREATE INDEX IF NOT EXISTS idx_create_date ON dwd.wholesale_jicai_volume_dtl (create_date);
CREATE INDEX IF NOT EXISTS idx_docid ON dwd.wholesale_jicai_volume_dtl (docid);
CREATE INDEX IF NOT EXISTS idx_dtlid ON dwd.wholesale_jicai_volume_dtl (dtlid);
CREATE INDEX IF NOT EXISTS idx_dlbegindate ON dwd.wholesale_jicai_volume_dtl (dlbegindate);
CREATE INDEX IF NOT EXISTS idx_dlenddate ON dwd.wholesale_jicai_volume_dtl (dlenddate);
CREATE INDEX IF NOT EXISTS idx_liangneiliangwai ON dwd.wholesale_jicai_volume_dtl (jicai_liangneiliangwai);