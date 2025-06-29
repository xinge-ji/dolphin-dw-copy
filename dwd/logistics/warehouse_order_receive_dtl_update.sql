INSERT INTO dwd.logistics_warehouse_order_receive_dtl (receiveid, __DORIS_DELETE_SIGN__)
SELECT a.receiveid, 1
FROM ods_wms.wms_in_order_dtl AS a
JOIN dwd.logistics_warehouse_order_receive_dtl AS b ON a.receiveid = b.receiveid
WHERE a.is_active = 0 AND a.dw_updatetime >= b.dw_updatetime;


INSERT INTO 
    dwd.logistics_warehouse_order_receive_dtl (
        receiveid,
        dw_updatetime,
        indtlid,
        warehid,
        warehouse_name,
        sectionid,
        section_name,
        goodsownerid,
        goodsowner_name,
        check_time,
        goodsid,
        goods_name,
        goods_category,
        scatter_qty,
        whole_qty
    )
-- 优化后的查询
WITH filtered_receive AS (
    SELECT receiveid, dw_updatetime, indtlid, warehid, sectionid, checkdate, goodsid, scatterqty, wholeqty
    FROM ods_wms.wms_receive_dtl 
    WHERE is_active = 1 
    AND dw_updatetime >= (SELECT MAX(dw_updatetime) - INTERVAL 60 DAY FROM dwd.logistics_warehouse_order_receive_dtl);
),
latest_shelf AS (
    -- 只处理有关联的shelf数据
    SELECT 
        sd.sourceid AS receiveid, 
        sd.section_name,
        ROW_NUMBER() OVER (PARTITION BY sd.sourceid ORDER BY sd.shelf_time ASC, sd.section_name ASC) as rn
    FROM dwd.logistics_warehouse_shelf_doc sd
    WHERE sd.section_name IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM filtered_receive fr WHERE fr.receiveid = sd.sourceid
      )
)
SELECT 
    r.receiveid,
    r.dw_updatetime,
    r.indtlid,
    r.warehid,
    d.warehouse_name,
    r.sectionid,
    COALESCE(j.sectionname, ls.section_name, '其他') AS section_name,
    d.goodsownerid,
    d.goodsowner_name,
    r.checkdate AS check_time,
    r.goodsid,
    d.goods_name,
    IFNULL(d.goods_category, '其他') AS goods_category,
    r.scatterqty,
    r.wholeqty
FROM filtered_receive r
JOIN dwd.logistics_warehouse_order_in_dtl d ON r.indtlid = d.indtlid
LEFT JOIN (SELECT sectionid, sectionname FROM ods_wms.wms_st_section_def WHERE is_active = 1) j ON r.sectionid = j.sectionid
LEFT JOIN (SELECT receiveid, section_name FROM latest_shelf WHERE rn = 1) ls ON r.receiveid = ls.receiveid;
