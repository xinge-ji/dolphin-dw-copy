DELETE FROM dwd.wholesale_jicai_cumulative_goods_qty
WHERE create_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY);

-- 插入最近60天的累计商品销售数量数据
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
        -- 获取最近60天内需要计算累计数量的维度组合
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
            AND i.credate >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)  -- 只处理最近60天的数据
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