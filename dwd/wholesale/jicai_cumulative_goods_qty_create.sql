DROP TABLE IF EXISTS dwd.wholesale_jicai_cumulative_goods_qty;

CREATE TABLE dwd.wholesale_jicai_cumulative_goods_qty (
    -- 维度信息
    entryid bigint COMMENT '独立单元ID',
    goodsid bigint COMMENT '商品ID',
    create_date datetime COMMENT '创建日期',
    
    -- 时间维度
    dlbegindate datetime COMMENT '带量开始日期',
    dlenddate datetime COMMENT '带量结束日期',

    -- 数量信息
    cumulative_goods_qty decimal(16,6) COMMENT '累计商品数量',
)
UNIQUE KEY(entryid, goodsid, create_date)
DISTRIBUTED BY HASH(entryid, goodsid)
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2"
);

-- =====================================================
-- 插入累计商品销售数量数据
-- =====================================================

INSERT INTO dwd.wholesale_jicai_cumulative_goods_qty (
    entryid,
    goodsid,
    dlbegindate,
    dlenddate,
    create_date,
    cumulative_goods_qty
)
SELECT 
    d.entryid,                  -- 独立单元ID
    d.goodsid,                  -- 商品ID
    d.dlbegindate,              -- 带量开始日期
    d.dlenddate,                -- 带量结束日期
    d.create_date,              -- 创建日期
    SUM(IFNULL(j.goodsqty, 0)) AS cumulative_goods_qty  -- 累计商品数量
FROM 
    (
        -- 获取所有需要计算累计数量的维度组合
        SELECT DISTINCT
            f.entryid,
            j.goodsid,
            f.dlbegindate,
            f.dlenddate,
            i.credate AS create_date
        FROM 
            ods_erp.bms_sa_doc i
        JOIN 
            ods_erp.bms_sa_dtl j ON i.salesid = j.salesid
        JOIN 
            ods_erp.t_101248_doc f ON i.entryid = f.entryid AND j.goodsid = f.goodsid
        WHERE 
            i.usestatus = 1
            AND i.credate BETWEEN f.dlbegindate AND f.dlenddate
            AND i.is_active = 1
            AND j.is_active = 1
            AND f.is_active = 1
    ) d
JOIN 
    ods_erp.bms_sa_doc i ON i.entryid = d.entryid
JOIN 
    ods_erp.bms_sa_dtl j ON i.salesid = j.salesid AND j.goodsid = d.goodsid
WHERE 
    i.usestatus = 1
    AND i.credate BETWEEN d.dlbegindate AND d.create_date  
    AND i.is_active = 1
    AND j.is_active = 1
GROUP BY 
    d.entryid, d.goodsid, d.dlbegindate, d.dlenddate, d.create_date;

CREATE INDEX IF NOT EXISTS idx_entryid ON dwd.wholesale_jicai_cumulative_goods_qty (entryid);
CREATE INDEX IF NOT EXISTS idx_goodsid ON dwd.wholesale_jicai_cumulative_goods_qty (goodsid);
CREATE INDEX IF NOT EXISTS idx_create_date ON dwd.wholesale_jicai_cumulative_goods_qty (create_date);
CREATE INDEX IF NOT EXISTS idx_dlbegindate ON dwd.wholesale_jicai_cumulative_goods_qty (dlbegindate);
CREATE INDEX IF NOT EXISTS idx_dlenddate ON dwd.wholesale_jicai_cumulative_goods_qty (dlenddate);