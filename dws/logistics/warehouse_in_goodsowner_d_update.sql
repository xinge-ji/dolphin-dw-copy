INSERT INTO dws.logistics_warehouse_in_goodsowner_d (
    -- 颗粒度
    stat_date,
    warehid,
    goodsownerid,
    category,

    -- 描述
    warehouse_name,
    goodsowner_name,
    
    -- 指标
    receive_count,
    check_count,
    check_scatter_count,
    check_whole_count,
    check_scatter_qty,
    check_whole_qty,
    flat_shelf_whole_qty,
    flat_shelf_scatter_count,
    auto_shelf_whole_qty,
    auto_shelf_scatter_count,
    ecode_count,
    udicode_count
)
WITH in_data AS (
    SELECT
        d.receive_time,
        d.warehid,
        d.goodsownerid,
        d.warehouse_name,
        d.goodsowner_name,
        d.inid,
        d.indtlid,
        d.goodsid,
        d.is_coldchain,
        d.is_chinese_medicine
    FROM 
        dwd.logistics_warehouse_order_in_dtl d
    JOIN 
        dwd.logistics_warehouse_order_in_doc doc ON d.inid = doc.inid
    WHERE 
        d.is_recheck = 0
        AND doc.is_autotask = 0
        AND d.receive_time is not NULL
        AND DATE(d.receive_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND DATE(d.receive_time) < CURRENT_DATE()
),
receive_data AS (
    SELECT 
        DATE(d.receive_time) AS stat_date,
        d.warehid,
        d.goodsownerid,
        d.warehouse_name,
        d.goodsowner_name,
        CASE 
            WHEN d.is_coldchain = 1 THEN '冷链'
            WHEN d.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END AS category,
        COUNT(1) AS receive_count
    FROM 
        in_data d
    GROUP BY 
        DATE(d.receive_time),
        d.warehid,
        d.goodsownerid,
        d.warehouse_name,
        d.goodsowner_name,
        CASE 
            WHEN d.is_coldchain = 1 THEN '冷链'
            WHEN d.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END
    
    UNION ALL
    
    SELECT 
        DATE(d.receive_time) AS stat_date,
        d.warehid,
        d.goodsownerid,
        d.warehouse_name,
        d.goodsowner_name,
        '总数' AS category,
        COUNT(1) AS receive_count
    FROM 
        in_data d
    GROUP BY 
        DATE(d.receive_time),
        d.warehid,
        d.goodsownerid,
        d.warehouse_name,
        d.goodsowner_name
),
check_data AS (
    SELECT 
        DATE(d.receive_time) AS stat_date,
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        CASE 
            WHEN d.is_coldchain = 1 THEN '冷链'
            WHEN d.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END AS category,
        COUNT(1) AS check_count
    FROM 
        dwd.logistics_warehouse_order_receive_dtl r
    JOIN 
        in_data d ON r.indtlid = d.indtlid
    GROUP BY 
        DATE(d.receive_time),
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        CASE 
            WHEN d.is_coldchain = 1 THEN '冷链'
            WHEN d.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END
    
    UNION ALL
    
    SELECT 
        DATE(d.receive_time) AS stat_date,
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        '总数' AS category,
        COUNT(1) AS check_count
    FROM 
        dwd.logistics_warehouse_order_receive_dtl r
    JOIN 
        in_data d ON r.indtlid = d.indtlid
    GROUP BY 
        DATE(d.receive_time),
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name
),
check_scatter_data AS (
    SELECT 
        DATE(d.receive_time) AS stat_date,
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        CASE 
            WHEN d.is_coldchain = 1 THEN '冷链'
            WHEN d.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END AS category,
        COUNT(1) AS check_scatter_count,
        SUM(r.scatter_qty) as check_scatter_qty
    FROM 
        dwd.logistics_warehouse_order_receive_dtl r
    JOIN 
        in_data d ON r.indtlid = d.indtlid
    WHERE r.scatter_qty is not null
    GROUP BY 
        DATE(d.receive_time),
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        CASE 
            WHEN d.is_coldchain = 1 THEN '冷链'
            WHEN d.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END
    
    UNION ALL
    
    SELECT 
        DATE(d.receive_time) AS stat_date,
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        '总数' AS category,
        COUNT(1) AS check_scatter_count,
        SUM(r.scatter_qty) as check_scatter_qty
    FROM 
        dwd.logistics_warehouse_order_receive_dtl r
    JOIN 
        in_data d ON r.indtlid = d.indtlid
    WHERE r.scatter_qty is not null
    GROUP BY 
        DATE(d.receive_time),
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name
),
check_whole_data AS (
    SELECT 
        DATE(d.receive_time) AS stat_date,
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        CASE 
            WHEN d.is_coldchain = 1 THEN '冷链'
            WHEN d.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END AS category,
        COUNT(1) AS check_whole_count,
        SUM(r.whole_qty) as check_whole_qty
    FROM 
        dwd.logistics_warehouse_order_receive_dtl r
    JOIN 
        in_data d ON r.indtlid = d.indtlid
    WHERE r.whole_qty is not null AND d.receive_time is not null
    GROUP BY 
        DATE(d.receive_time),
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        CASE 
            WHEN d.is_coldchain = 1 THEN '冷链'
            WHEN d.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END
    
    UNION ALL
    
    SELECT 
        DATE(d.receive_time) AS stat_date,
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        '总数' AS category,
        COUNT(1) AS check_whole_count,
        SUM(r.whole_qty) as check_whole_qty
    FROM 
        dwd.logistics_warehouse_order_receive_dtl r
    JOIN 
        in_data d ON r.indtlid = d.indtlid
    WHERE r.whole_qty is not null AND d.receive_time is not null
    GROUP BY 
        DATE(d.receive_time),
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name
),
flat_shelf_data AS (
    SELECT 
        DATE(io.finish_time) AS stat_date,
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        CASE 
            WHEN io.is_coldchain = 1 THEN '冷链'
            WHEN io.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END AS category,
        SUM(io.whole_qty) AS flat_shelf_whole_qty,
        SUM(CASE WHEN io.scatter_qty is not NULL THEN 1 ELSE 0 END) AS flat_shelf_scatter_count
    FROM 
        dwd.logistics_warehouse_st_io_doc io
    WHERE 
        io.is_out = 0  -- 入库单
        AND io.comefrom = 1  -- 来源为收货
        AND io.rfmanid != 0  -- 非系统管理员
        AND io.finish_time IS NOT NULL  -- 已完成的单据
        AND io.sectionid != 8515  -- 非立库
        AND DATE(io.finish_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND DATE(io.finish_time) < CURRENT_DATE()
    GROUP BY 
        DATE(io.finish_time),
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        CASE 
            WHEN io.is_coldchain = 1 THEN '冷链'
            WHEN io.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END
    
    UNION ALL
    
    SELECT 
        DATE(io.finish_time) AS stat_date,
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name,
        '总数' AS category,
        SUM(io.whole_qty) AS flat_shelf_whole_qty,
        SUM(CASE WHEN io.scatter_qty is not NULL THEN 1 ELSE 0 END) AS flat_shelf_scatter_count
    FROM 
        dwd.logistics_warehouse_st_io_doc io
    WHERE 
        io.is_out = 0  -- 入库单
        AND io.comefrom = 1  -- 来源为收货
        AND io.rfmanid != 0  -- 非系统管理员
        AND io.finish_time IS NOT NULL  -- 已完成的单据
        AND io.sectionid != 8515   -- 非立库
        AND DATE(io.finish_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND DATE(io.finish_time) < CURRENT_DATE()
    GROUP BY 
        DATE(io.finish_time),
        io.warehid,
        io.goodsownerid,
        io.warehouse_name,
        io.goodsowner_name
),
auto_shelf_data AS (
    SELECT 
        DATE(d.receive_time) AS stat_date,
        d.warehid,
        d.goodsownerid,
        d.warehouse_name,
        d.goodsowner_name,
        CASE 
            WHEN d.is_coldchain = 1 THEN '冷链'
            WHEN d.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END AS category,
        SUM(z.wholeqty) AS auto_shelf_whole_qty,
        SUM(z.scattercount) AS auto_shelf_scatter_count
    FROM 
        dwd.logistics_warehouse_order_in_dtl d
    JOIN
        ods_wms.zx_19007_v z ON d.indtlid = z.indtlid
    WHERE d.receive_time is not null
        AND DATE(d.receive_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND DATE(d.receive_time) < CURRENT_DATE()
    GROUP BY 
        DATE(d.receive_time),
        d.warehid,
        d.goodsownerid,
        d.warehouse_name,
        d.goodsowner_name,
        CASE 
            WHEN d.is_coldchain = 1 THEN '冷链'
            WHEN d.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END
    
    UNION ALL
    
    SELECT 
        DATE(d.receive_time) AS stat_date,
        d.warehid,
        d.goodsownerid,
        d.warehouse_name,
        d.goodsowner_name,
        '总数' AS category,
        SUM(z.wholeqty) AS auto_shelf_whole_qty,
        SUM(z.scattercount) AS auto_shelf_scatter_count
    FROM 
        dwd.logistics_warehouse_order_in_dtl d
    JOIN
        ods_wms.zx_19007_v z ON d.indtlid = z.indtlid
    WHERE d.receive_time is not null
        AND DATE(d.receive_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND DATE(d.receive_time) < CURRENT_DATE()
    GROUP BY 
        DATE(d.receive_time),
        d.warehid,
        d.goodsownerid,
        d.warehouse_name,
        d.goodsowner_name
),
ecode_data AS (
    SELECT 
        DATE(e.create_time) AS stat_date,
        e.warehid,
        e.goodsownerid,
        e.warehouse_name,
        e.goodsowner_name,
        CASE 
            WHEN e.is_coldchain = 1 THEN '冷链'
            WHEN e.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END AS category,
        COUNT(1) AS ecode_count
    FROM 
        dwd.logistics_warehouse_ecode e 
    WHERE 
        e.is_out = 0
        AND DATE(e.create_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND DATE(e.create_time) < CURRENT_DATE()
    GROUP BY 
        DATE(e.create_time),
        e.warehid,
        e.goodsownerid,
        e.warehouse_name,
        e.goodsowner_name,
        CASE 
            WHEN e.is_coldchain = 1 THEN '冷链'
            WHEN e.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END
    
    UNION ALL
    
    SELECT 
        DATE(e.create_time) AS stat_date,
        e.warehid,
        e.goodsownerid,
        e.warehouse_name,
        e.goodsowner_name,
        '总数' AS category,
        COUNT(1) AS ecode_count
    FROM 
        dwd.logistics_warehouse_ecode e
    WHERE 
        e.is_out = 0  -- 入库单
        AND DATE(e.create_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND DATE(e.create_time) < CURRENT_DATE()
    GROUP BY 
        DATE(e.create_time),
        e.warehid,
        e.goodsownerid,
        e.warehouse_name,
        e.goodsowner_name
),
udicode_data AS (
    SELECT 
        DATE(e.create_time) AS stat_date,
        e.warehid,
        e.goodsownerid,
        e.warehouse_name,
        e.goodsowner_name,
        CASE 
            WHEN e.is_coldchain = 1 THEN '冷链'
            WHEN e.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END AS category,
        COUNT(1) AS udicode_count
    FROM 
        dwd.logistics_warehouse_udicode e
    WHERE 
        e.is_out = 0  -- 入库单
        AND e.create_time IS NOT NULL
        AND DATE(e.create_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND DATE(e.create_time) < CURRENT_DATE()
    GROUP BY 
        DATE(e.create_time),
        e.warehid,
        e.goodsownerid,
        e.warehouse_name,
        e.goodsowner_name,
        CASE 
            WHEN e.is_coldchain = 1 THEN '冷链'
            WHEN e.is_chinese_medicine = 1 THEN '中药'
            ELSE '其他'
        END
    
    UNION ALL
    
    SELECT 
        DATE(e.create_time) AS stat_date,
        e.warehid,
        e.goodsownerid,
        e.warehouse_name,
        e.goodsowner_name,
        '总数' AS category,
        COUNT(1) AS udicode_count
    FROM 
        dwd.logistics_warehouse_udicode e
    WHERE 
        e.is_out = 0  -- 入库单
        AND e.create_time IS NOT NULL
        AND DATE(e.create_time) >= CURRENT_DATE() - INTERVAL 60 DAY
        AND DATE(e.create_time) < CURRENT_DATE()
    GROUP BY 
        DATE(e.create_time),
        e.warehid,
        e.goodsownerid,
        e.warehouse_name,
        e.goodsowner_name
),
union_keys AS (
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM receive_data
    UNION
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM check_data
    UNION
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM check_scatter_data
    UNION
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM check_whole_data
    UNION
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM flat_shelf_data
    UNION
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM auto_shelf_data
    UNION
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM ecode_data
    UNION
    SELECT stat_date, warehid, goodsownerid, category, warehouse_name, goodsowner_name FROM udicode_data
)
-- 最终SELECT部分：
SELECT 
    k.stat_date,
    k.warehid,
    k.goodsownerid,
    k.category,
    k.warehouse_name,
    k.goodsowner_name,
    COALESCE(r.receive_count, 0) AS receive_count,
    COALESCE(c.check_count, 0) AS check_count,
    COALESCE(cs.check_scatter_count, 0) AS check_scatter_count,
    COALESCE(cw.check_whole_count, 0) AS check_whole_count,
    COALESCE(cs.check_scatter_qty, 0) AS check_scatter_qty,
    COALESCE(cw.check_whole_qty, 0) AS check_whole_qty,
    COALESCE(fsd.flat_shelf_whole_qty, 0) AS flat_shelf_whole_qty,
    COALESCE(fsd.flat_shelf_scatter_count, 0) AS flat_shelf_scatter_count,
    COALESCE(asd.auto_shelf_whole_qty, 0) AS auto_shelf_whole_qty,
    COALESCE(asd.auto_shelf_scatter_count, 0) AS auto_shelf_scatter_count,
    COALESCE(e.ecode_count, 0) AS ecode_count,
    COALESCE(u.udicode_count, 0) AS udicode_count
FROM union_keys k
LEFT JOIN receive_data r ON k.stat_date = r.stat_date AND k.warehid = r.warehid AND k.goodsownerid = r.goodsownerid AND k.category = r.category
LEFT JOIN check_data c ON k.stat_date = c.stat_date AND k.warehid = c.warehid AND k.goodsownerid = c.goodsownerid AND k.category = c.category
LEFT JOIN check_scatter_data cs ON k.stat_date = cs.stat_date AND k.warehid = cs.warehid AND k.goodsownerid = cs.goodsownerid AND k.category = cs.category
LEFT JOIN check_whole_data cw ON k.stat_date = cw.stat_date AND k.warehid = cw.warehid AND k.goodsownerid = cw.goodsownerid AND k.category = cw.category
LEFT JOIN flat_shelf_data fsd ON k.stat_date = fsd.stat_date AND k.warehid = fsd.warehid AND k.goodsownerid = fsd.goodsownerid AND k.category = fsd.category
LEFT JOIN auto_shelf_data asd ON k.stat_date = asd.stat_date AND k.warehid = asd.warehid AND k.goodsownerid = asd.goodsownerid AND k.category = asd.category
LEFT JOIN ecode_data e ON k.stat_date = e.stat_date AND k.warehid = e.warehid AND k.goodsownerid = e.goodsownerid AND k.category = e.category
LEFT JOIN udicode_data u ON k.stat_date = u.stat_date AND k.warehid = u.warehid AND k.goodsownerid = u.goodsownerid AND k.category = u.category
WHERE k.warehid is not NULL and k.goodsownerid is not NULL;