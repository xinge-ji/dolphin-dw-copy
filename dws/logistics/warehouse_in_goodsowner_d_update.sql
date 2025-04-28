
INSERT INTO dws.logistics_warehouse_in_goodsowner_d (
    -- 颗粒度
    stat_date,
    warehid,
    goodsownerid,
    
    -- 维度
    warehouse_name,
    goodsowner_name,
    
    -- 指标 - 收货相关
    receive_item_count,
    receive_coldchain_item_count,
    receive_chinesemedicine_item_count,
    receive_other_item_count,
    
    -- 指标 - 验收相关
    check_item_count,
    check_coldchain_item_count,
    check_chinesemedicine_item_count,
    check_other_item_count,
    
    -- 指标 - 平库上架相关
    flat_shelf_whole_qty,
    flat_shelf_coldchain_whole_qty,
    flat_shelf_chinesemedicine_whole_qty,
    flat_shelf_other_whole_qty,
    
    flat_shelf_scatter_qty,
    flat_shelf_coldchain_scatter_qty,
    flat_shelf_chinesemedicine_scatter_qty,
    flat_shelf_other_scatter_qty,
    
    -- 指标 - 立库上架相关
    auto_shelf_whole_qty,
    auto_shelf_coldchain_whole_qty,
    auto_shelf_chinesemedicine_whole_qty,
    auto_shelf_other_whole_qty,
    
    auto_shelf_scatter_qty,
    auto_shelf_coldchain_scatter_qty,
    auto_shelf_chinesemedicine_scatter_qty,
    auto_shelf_other_scatter_qty,

    -- 指标 - 平库电子监管码相关
    flat_ecode_count,
    flat_coldchain_ecode_count,
    flat_chinesemedicine_ecode_count,
    flat_other_ecode_count,

    -- 指标 - 立库电子监管码相关
    auto_ecode_count,
    auto_coldchain_ecode_count,
    auto_chinesemedicine_ecode_count,
    auto_other_ecode_count,

    -- 指标 - 平库UDI码相关
    flat_udicode_count,
    flat_coldchain_udicode_count,
    flat_chinesemedicine_udicode_count,
    flat_other_udicode_count,

    -- 指标 - 立库UDI码相关
    auto_udicode_count,
    auto_coldchain_udicode_count,
    auto_chinesemedicine_udicode_count,
    auto_other_udicode_count
)
WITH 
in_data AS (
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
        AND doc.usestatus = 3
        AND d.receive_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
),
receive_data AS (
    SELECT 
        DATE(d.receive_time) AS stat_date,
        d.warehid,
        d.goodsownerid,
        d.warehouse_name,
        d.goodsowner_name,
        COUNT(1) AS receive_item_count,
        SUM(d.is_coldchain) AS receive_coldchain_item_count,
        SUM(d.is_chinese_medicine) AS receive_chinesemedicine_item_count,
        COUNT(1) - SUM(d.is_coldchain) - SUM(d.is_chinese_medicine) AS receive_other_item_count
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
        COUNT(1) AS check_item_count,
        SUM(d.is_coldchain) AS check_coldchain_item_count,
        SUM(d.is_chinese_medicine) AS check_chinesemedicine_item_count,
        COUNT(1) - SUM(d.is_coldchain) - SUM(d.is_chinese_medicine) AS check_other_item_count
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
shelf_data AS (
    SELECT 
        DATE(io.finish_time) AS stat_date,
        io.warehid,
        doc.goodsownerid,
        io.warehouse_name,
        doc.goodsowner_name,
        -- 平库上架整件件数
        SUM(CASE WHEN io.sectionid != 8515 THEN io.whole_qty ELSE 0 END) AS flat_shelf_whole_qty,
        SUM(CASE WHEN io.sectionid != 8515 AND io.is_coldchain = 1 THEN io.whole_qty ELSE 0 END) AS flat_shelf_coldchain_whole_qty,
        SUM(CASE WHEN io.sectionid != 8515 AND io.is_chinese_medicine = 1 THEN io.whole_qty ELSE 0 END) AS flat_shelf_chinesemedicine_whole_qty,
        SUM(CASE 
            WHEN io.sectionid != 8515 AND io.is_coldchain = 0 AND io.is_chinese_medicine = 0 
            THEN io.whole_qty ELSE 0 
        END) AS flat_shelf_other_whole_qty,
        
        -- 平库上架散件件数
        COUNT(CASE WHEN io.sectionid != 8515 AND io.scatter_qty > 0 THEN 1 ELSE NULL END) AS flat_shelf_scatter_qty,
        COUNT(CASE WHEN io.sectionid != 8515 AND io.is_coldchain = 1 AND io.scatter_qty > 0 THEN 1 ELSE NULL END) AS flat_shelf_coldchain_scatter_qty,
        COUNT(CASE WHEN io.sectionid != 8515 AND io.is_chinese_medicine = 1 AND io.scatter_qty > 0 THEN 1 ELSE NULL END) AS flat_shelf_chinesemedicine_scatter_qty,
        COUNT(CASE 
            WHEN io.sectionid != 8515 AND io.is_coldchain = 0 AND io.is_chinese_medicine = 0 AND io.scatter_qty > 0
            THEN 1 ELSE NULL 
        END) AS flat_shelf_other_scatter_qty,
        
        -- 立库上架整件件数
        SUM(CASE WHEN io.sectionid = 8515 THEN io.whole_qty ELSE 0 END) AS auto_shelf_whole_qty,
        SUM(CASE WHEN io.sectionid = 8515 AND io.is_coldchain = 1 THEN io.whole_qty ELSE 0 END) AS auto_shelf_coldchain_whole_qty,
        SUM(CASE WHEN io.sectionid = 8515 AND io.is_chinese_medicine = 1 THEN io.whole_qty ELSE 0 END) AS auto_shelf_chinesemedicine_whole_qty,
        SUM(CASE 
            WHEN io.sectionid = 8515 AND io.is_coldchain = 0 AND io.is_chinese_medicine = 0 
            THEN io.whole_qty ELSE 0 
        END) AS auto_shelf_other_whole_qty,
        
        -- 立库上架散件件数
        COUNT(CASE WHEN io.sectionid = 8515 AND io.scatter_qty > 0 THEN 1 ELSE NULL END) AS auto_shelf_scatter_qty,
        COUNT(CASE WHEN io.sectionid = 8515 AND io.is_coldchain = 1 AND io.scatter_qty > 0 THEN 1 ELSE NULL END) AS auto_shelf_coldchain_scatter_qty,
        COUNT(CASE WHEN io.sectionid = 8515 AND io.is_chinese_medicine = 1 AND io.scatter_qty > 0 THEN 1 ELSE NULL END) AS auto_shelf_chinesemedicine_scatter_qty,
        COUNT(CASE 
            WHEN io.sectionid = 8515 AND io.is_coldchain = 0 AND io.is_chinese_medicine = 0 AND io.scatter_qty > 0
            THEN 1 ELSE NULL 
        END) AS auto_shelf_other_scatter_qty
    FROM 
        dwd.logistics_warehouse_st_io_doc io
    LEFT JOIN 
        dwd.logistics_warehouse_order_in_doc doc ON io.sourceid = doc.inid
    WHERE 
        io.is_out = 0  -- 入库单
        AND io.comefrom = 1  -- 来源为收货
        AND io.rfmanid != 0  -- 非系统管理员
        AND io.finish_time IS NOT NULL  -- 已完成的单据
    GROUP BY 
        DATE(io.finish_time),
        io.warehid,
        doc.goodsownerid,
        io.warehouse_name,
        doc.goodsowner_name
),
ecode_data AS (
    SELECT 
        DATE(r.check_time) AS stat_date,
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        -- 平库电子监管码
        COUNT(DISTINCT CASE WHEN io.sectionid != 8515 THEN e.ecode ELSE NULL END) AS flat_ecode_count,
        COUNT(DISTINCT CASE WHEN io.sectionid != 8515 AND d.is_coldchain = 1 THEN e.ecode ELSE NULL END) AS flat_coldchain_ecode_count,
        COUNT(DISTINCT CASE WHEN io.sectionid != 8515 AND d.is_chinese_medicine = 1 THEN e.ecode ELSE NULL END) AS flat_chinesemedicine_ecode_count,
        COUNT(DISTINCT CASE 
            WHEN io.sectionid != 8515 AND d.is_coldchain = 0 AND d.is_chinese_medicine = 0 
            THEN e.ecode ELSE NULL END) AS flat_other_ecode_count,
        
        -- 立库电子监管码
        COUNT(DISTINCT CASE WHEN io.sectionid = 8515 THEN e.ecode ELSE NULL END) AS auto_ecode_count,
        COUNT(DISTINCT CASE WHEN io.sectionid = 8515 AND d.is_coldchain = 1 THEN e.ecode ELSE NULL END) AS auto_coldchain_ecode_count,
        COUNT(DISTINCT CASE WHEN io.sectionid = 8515 AND d.is_chinese_medicine = 1 THEN e.ecode ELSE NULL END) AS auto_chinesemedicine_ecode_count,
        COUNT(DISTINCT CASE 
            WHEN io.sectionid = 8515 AND d.is_coldchain = 0 AND d.is_chinese_medicine = 0 
            THEN e.ecode ELSE NULL END) AS auto_other_ecode_count
    FROM 
        dwd.logistics_warehouse_ecode e
    JOIN 
        dwd.logistics_warehouse_order_receive_dtl r ON e.sourceid = r.indtlid
    JOIN
        dwd.logistics_warehouse_st_io_doc io ON io.sourceid = r.receiveid
    JOIN 
        in_data d ON r.indtlid = d.indtlid
    GROUP BY 
        DATE(r.check_time),
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name
),
udicode_data AS (
    SELECT 
        DATE(r.check_time) AS stat_date,
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name,
        -- 平库UDI码
        COUNT(DISTINCT CASE WHEN io.sectionid != 8515 THEN u.udicode ELSE NULL END) AS flat_udicode_count,
        COUNT(DISTINCT CASE WHEN io.sectionid != 8515 AND d.is_coldchain = 1 THEN u.udicode ELSE NULL END) AS flat_coldchain_udicode_count,
        COUNT(DISTINCT CASE WHEN io.sectionid != 8515 AND d.is_chinese_medicine = 1 THEN u.udicode ELSE NULL END) AS flat_chinesemedicine_udicode_count,
        COUNT(DISTINCT CASE 
            WHEN io.sectionid != 8515 AND d.is_coldchain = 0 AND d.is_chinese_medicine = 0 
            THEN u.udicode ELSE NULL END) AS flat_other_udicode_count,
        
        -- 立库UDI码
        COUNT(DISTINCT CASE WHEN io.sectionid = 8515 THEN u.udicode ELSE NULL END) AS auto_udicode_count,
        COUNT(DISTINCT CASE WHEN io.sectionid = 8515 AND d.is_coldchain = 1 THEN u.udicode ELSE NULL END) AS auto_coldchain_udicode_count,
        COUNT(DISTINCT CASE WHEN io.sectionid = 8515 AND d.is_chinese_medicine = 1 THEN u.udicode ELSE NULL END) AS auto_chinesemedicine_udicode_count,
        COUNT(DISTINCT CASE 
            WHEN io.sectionid = 8515 AND d.is_coldchain = 0 AND d.is_chinese_medicine = 0 
            THEN u.udicode ELSE NULL END) AS auto_other_udicode_count
    FROM 
        dwd.logistics_warehouse_udicode u
    JOIN 
        dwd.logistics_warehouse_order_receive_dtl r ON u.sourceid = r.indtlid
    JOIN
        dwd.logistics_warehouse_st_io_doc io ON io.sourceid = r.receiveid
    JOIN 
        in_data d ON r.indtlid = d.indtlid
    GROUP BY 
        DATE(r.check_time),
        r.warehid,
        d.goodsownerid,
        r.warehouse_name,
        d.goodsowner_name
)
SELECT 
    COALESCE(r.stat_date, COALESCE(c.stat_date, COALESCE(s.stat_date, COALESCE(e.stat_date, u.stat_date)))) AS stat_date,
    COALESCE(r.warehid, COALESCE(c.warehid, COALESCE(s.warehid, COALESCE(e.warehid, u.warehid)))) AS warehid,
    COALESCE(r.goodsownerid, COALESCE(c.goodsownerid, COALESCE(s.goodsownerid, COALESCE(e.goodsownerid, u.goodsownerid)))) AS goodsownerid,
    COALESCE(r.warehouse_name, COALESCE(c.warehouse_name, COALESCE(s.warehouse_name, COALESCE(e.warehouse_name, u.warehouse_name)))) AS warehouse_name,
    COALESCE(r.goodsowner_name, COALESCE(c.goodsowner_name, COALESCE(s.goodsowner_name, COALESCE(e.goodsowner_name, u.goodsowner_name)))) AS goodsowner_name,
    
    -- 收货指标
    COALESCE(r.receive_item_count, 0) AS receive_item_count,
    COALESCE(r.receive_coldchain_item_count, 0) AS receive_coldchain_item_count,
    COALESCE(r.receive_chinesemedicine_item_count, 0) AS receive_chinesemedicine_item_count,
    COALESCE(r.receive_other_item_count, 0) AS receive_other_item_count,
    
    -- 验收指标
    COALESCE(c.check_item_count, 0) AS check_item_count,
    COALESCE(c.check_coldchain_item_count, 0) AS check_coldchain_item_count,
    COALESCE(c.check_chinesemedicine_item_count, 0) AS check_chinesemedicine_item_count,
    COALESCE(c.check_other_item_count, 0) AS check_other_item_count,
    
    -- 平库上架指标 - 整件
    COALESCE(s.flat_shelf_whole_qty, 0) AS flat_shelf_whole_qty,
    COALESCE(s.flat_shelf_coldchain_whole_qty, 0) AS flat_shelf_coldchain_whole_qty,
    COALESCE(s.flat_shelf_chinesemedicine_whole_qty, 0) AS flat_shelf_chinesemedicine_whole_qty,
    COALESCE(s.flat_shelf_other_whole_qty, 0) AS flat_shelf_other_whole_qty,
    
    -- 平库上架指标 - 散件
    COALESCE(s.flat_shelf_scatter_qty, 0) AS flat_shelf_scatter_qty,
    COALESCE(s.flat_shelf_coldchain_scatter_qty, 0) AS flat_shelf_coldchain_scatter_qty,
    COALESCE(s.flat_shelf_chinesemedicine_scatter_qty, 0) AS flat_shelf_chinesemedicine_scatter_qty,
    COALESCE(s.flat_shelf_other_scatter_qty, 0) AS flat_shelf_other_scatter_qty,
    
    -- 立库上架指标 - 整件
    COALESCE(s.auto_shelf_whole_qty, 0) AS auto_shelf_whole_qty,
    COALESCE(s.auto_shelf_coldchain_whole_qty, 0) AS auto_shelf_coldchain_whole_qty,
    COALESCE(s.auto_shelf_chinesemedicine_whole_qty, 0) AS auto_shelf_chinesemedicine_whole_qty,
    COALESCE(s.auto_shelf_other_whole_qty, 0) AS auto_shelf_other_whole_qty,
    
    -- 立库上架指标 - 散件
    COALESCE(s.auto_shelf_scatter_qty, 0) AS auto_shelf_scatter_qty,
    COALESCE(s.auto_shelf_coldchain_scatter_qty, 0) AS auto_shelf_coldchain_scatter_qty,
    COALESCE(s.auto_shelf_chinesemedicine_scatter_qty, 0) AS auto_shelf_chinesemedicine_scatter_qty,
    COALESCE(s.auto_shelf_other_scatter_qty, 0) AS auto_shelf_other_scatter_qty,
        
    -- 平库电子监管码指标
    COALESCE(e.flat_ecode_count, 0) AS flat_ecode_count,
    COALESCE(e.flat_coldchain_ecode_count, 0) AS flat_coldchain_ecode_count,
    COALESCE(e.flat_chinesemedicine_ecode_count, 0) AS flat_chinesemedicine_ecode_count,
    COALESCE(e.flat_other_ecode_count, 0) AS flat_other_ecode_count,
    
    -- 立库电子监管码指标
    COALESCE(e.auto_ecode_count, 0) AS auto_ecode_count,
    COALESCE(e.auto_coldchain_ecode_count, 0) AS auto_coldchain_ecode_count,
    COALESCE(e.auto_chinesemedicine_ecode_count, 0) AS auto_chinesemedicine_ecode_count,
    COALESCE(e.auto_other_ecode_count, 0) AS auto_other_ecode_count,
    
    -- 平库UDI码指标
    COALESCE(u.flat_udicode_count, 0) AS flat_udicode_count,
    COALESCE(u.flat_coldchain_udicode_count, 0) AS flat_coldchain_udicode_count,
    COALESCE(u.flat_chinesemedicine_udicode_count, 0) AS flat_chinesemedicine_udicodee_count,
    COALESCE(u.flat_other_udicode_count, 0) AS flat_other_udicode_count,
    
    -- 立库UDI码指标
    COALESCE(u.auto_udicode_count, 0) AS auto_udicode_count,
    COALESCE(u.auto_coldchain_udicode_count, 0) AS auto_coldchain_udicode_count,
    COALESCE(u.auto_chinesemedicine_udicode_count, 0) AS auto_chinesemedicine_udicodee_count,
    COALESCE(u.auto_other_udicode_count, 0) AS auto_other_udicode_count
FROM 
    receive_data r
FULL JOIN 
    check_data c ON r.stat_date = c.stat_date AND r.warehid = c.warehid AND r.goodsownerid = c.goodsownerid
FULL JOIN 
    shelf_data s ON r.stat_date = s.stat_date AND r.warehid = s.warehid AND r.goodsownerid = s.goodsownerid
FULL JOIN 
    ecode_data e ON r.stat_date = e.stat_date AND r.warehid = e.warehid AND r.goodsownerid = e.goodsownerid
FULL JOIN 
    udicode_data u ON r.stat_date = u.stat_date AND r.warehid = u.warehid AND r.goodsownerid = u.goodsownerid;