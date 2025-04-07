DELETE FROM dwd.wholesale_jicai_volume_dtl
WHERE create_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY);

-- 来自 ERP V_LSQK_DL
-- 插入最近60天的集采带量数据
INSERT INTO dwd.wholesale_jicai_volume_dtl (
    entryid,
    customid,
    salesid,
    goodsid,
    create_date,
    docid,
    dtlid,
    jicai_batch_name,
    dlbegindate,
    dlenddate,
    reported_qty,
    contract_payment_days,
    total_goods_qty,
    excess_payment_days,
    jicai_liangneiliangwai,
    guankong_type
)
SELECT 
    -- 基础维度信息
    k.entryid,                  -- 独立单元ID
    k.customid,                 -- 客户ID
    k.salesid,                  -- 销售单ID
    k.goodsid,                  -- 商品ID
    
    -- 时间信息
    k.credate AS create_date,   -- 创建日期
    
    -- 集采相关信息
    k.docid,                    -- 集采带量单据ID
    k.dtlid,                    -- 带量明细ID
    k.dlmcpc AS jicai_batch_name, -- 带量名称批次
    k.dlbegindate,              -- 带量开始日期
    k.dlenddate,                -- 带量结束日期
    
    CAST(k.blsl as decimal(16,6)) AS reported_qty,     -- 报量数量
    
    -- 回款相关
    CASE
        WHEN k.gklx = 2 THEN k.xyhkts                  -- 管控类型为2时，使用协议回款天数
        ELSE k.hkkdy * 30 - 15 + k.pdrq                -- 否则根据回款跨度月和判断日期计算
    END AS contract_payment_days,                      -- 协议回款天数
    
    k.ljxssl AS total_goods_qty, -- 累计销售数量
    
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
        END AS flag
    FROM (
        -- 子查询：获取带量超期欠款基础数据
        SELECT 
            d.entryid,          -- 独立单元ID
            d.customid,         -- 客户ID
            d.salesid,          -- 销售单ID
            e.goodsid,          -- 商品ID
            d.credate,          -- 创建日期
            
            f.docid,            -- 集采带量单据ID
            f.jczbj,            -- 集采中标价
            f.dlmcpc,           -- 带量名称批次
            f.dlbegindate,      -- 带量开始日期
            f.dlenddate,        -- 带量结束日期
            f.xyhkts,           -- 协议回款天数
            
            IFNULL(cs.cumulative_goods_qty, 0) AS ljxssl, -- 累计销售数量
            
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
            dwd.wholesale_jicai_cumulative_sales_qty cs ON d.entryid = cs.entryid 
                                                       AND e.goodsid = cs.goodsid 
                                                       AND d.credate = cs.create_date
        WHERE 
            IFNULL(e.total_line, 0) - IFNULL(e.totalrecmoney, 0) <> 0
            AND d.credate BETWEEN f.dlbegindate AND f.dlenddate
            AND d.credate >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)  -- 只处理最近60天的数据
            AND f.usestatus = 1
            AND d.is_active = 1
            AND e.is_active = 1
            AND f.is_active = 1
    ) g
    LEFT JOIN 
        ods_erp.t_101248_dtl h ON g.docid = h.docid AND g.customid = h.customid AND h.is_active = 1
) k;