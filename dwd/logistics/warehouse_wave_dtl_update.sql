INSERT INTO dwd.logistics_warehouse_wave_dtl (
    waveid,
    wavedtlid,
    dw_updatetime,
    create_time,
    print_time,
    warehid,
    warehouse_name,
    goodsownerid,
    goodsowner_name,
    out_scatter_box_count,
    out_whole_qty
)
WITH wholeqty_calc AS (
    SELECT
        t2.wavedtlid,
        IFNULL(SUM(IFNULL(t1.wholeqty,0)) + SUM(IFNULL(t1.iwcs_wholeqty,0)), 0) AS wholeqty
    FROM ods_wms.wms_out_order_lot_dtl t1
    JOIN ods_wms.wms_wave_goods_dtl t2 ON t1.wavegoodsdtlid = t2.wavegoodsdtlid
    JOIN ods_wms.wms_wave_dtl t3 ON t2.wavedtlid=t3.wavedtlid
    JOIN dwd.logistics_warehouse_wave_doc t4 ON t3.waveid = t4.wavedocid
    WHERE t1.is_active = 1 AND t2.is_active = 1 AND t3.is_active = 1 AND t4.create_time >= (CURRENT_DATE() - INTERVAL 60 DAY)
    GROUP BY t2.wavedtlid
),
boxno_iwcs AS (
    SELECT
        b.wavedtlid,
        IFNULL(GROUP_CONCAT(a.iwcs_boxno ORDER BY a.iwcs_boxno), '0') AS m_boxseq
    FROM ods_wms.wms_out_order_lot_dtl a
    JOIN ods_wms.wms_wave_goods_dtl b ON a.wavegoodsdtlid = b.wavegoodsdtlid
    JOIN ods_wms.wms_st_io_doc c ON b.wavegoodsdtlid = c.sourceid AND c.comefrom = 3
    JOIN ods_wms.wms_wave_dtl t3 ON b.wavedtlid=t3.wavedtlid
    JOIN dwd.logistics_warehouse_wave_doc t4 ON t3.waveid = t4.wavedocid
    WHERE a.is_active = 1 AND b.is_active = 1 AND c.is_active = 1 AND t3.is_active = 1 AND t4.create_time >= (CURRENT_DATE() - INTERVAL 60 DAY)
    GROUP BY b.wavedtlid
),
boxno_iwcs_count AS (
    SELECT
        i.wavedtlid,
        IF(i.m_boxseq = '0' OR i.m_boxseq IS NULL, 0, array_size(split_by_string(i.m_boxseq, ','))) AS iwcsnum
    FROM boxno_iwcs i
),
boxno_wms_count AS (
    SELECT
        t.wavedtlid,
        IFNULL(COUNT(1), 0) AS wmsnum
    FROM ods_wms.wms_box_doc t
    WHERE (t.boxmanid <> 0 OR (t.boxmanid = 0 AND IFNULL(t.autofinishflag, 0) = 1)) AND t.is_active = 1
    GROUP BY t.wavedtlid
),
boxno_total AS (
    SELECT
        COALESCE(i.wavedtlid, w.wavedtlid) AS wavedtlid,
        IFNULL(i.iwcsnum, 0) + IFNULL(w.wmsnum, 0) AS boxnum
    FROM boxno_iwcs_count i
    FULL OUTER JOIN boxno_wms_count w ON i.wavedtlid = w.wavedtlid
)
SELECT
    a.waveid,
    a.wavedtlid,
    a.dw_updatetime,
    CASE WHEN g.wavedocid = 0 THEN a.autofinishdate ELSE g.create_time END AS create_time,
    a.printdate AS print_time,
    g.warehid,
    g.warehouse_name,
    a.goodsownerid,
    e.goodsownername AS goodsowner_name,
    CASE WHEN g.warehid IN (1, 17)
        THEN IFNULL(bt.boxnum, a.scatterfinpacks)
        ELSE a.scatterfinpacks
    END AS out_scatter_box_count,
    CASE WHEN g.warehid IN (1, 17)
        THEN IFNULL(wc.wholeqty, a.wholefinpacks)
        ELSE a.wholefinpacks
    END AS out_whole_qty
FROM ods_wms.wms_wave_dtl a
JOIN dwd.logistics_warehouse_wave_doc g ON a.waveid = g.wavedocid
LEFT JOIN ods_wms.tpl_goodsowner e ON a.goodsownerid = e.goodsownerid
LEFT JOIN boxno_total bt ON a.wavedtlid = bt.wavedtlid
LEFT JOIN wholeqty_calc wc ON a.wavedtlid = wc.wavedtlid
WHERE a.is_active = 1 AND e.is_active = 1 AND g.create_time >= (CURRENT_DATE() - INTERVAL 60 DAY);