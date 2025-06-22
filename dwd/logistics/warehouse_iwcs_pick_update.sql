TRUNCATE TABLE dwd.logistics_warehouse_iwcs_pick;
INSERT INTO 
    dwd.logistics_warehouse_iwcs_pick (
        ssc_picking_carton_id,
        create_time,
        pick_time,
        check_time,
        wms_inout_id,
        carton_type,
        whole_qty,
        scatter_count,
        scatter_qty
    )
SELECT 
    ssc_picking_carton_id,
    create_date,
    pick_time,
    check_time,
    wms_inout_id,
    CASE 
        WHEN carton_type = 'A' THEN '零散出库'
        WHEN carton_type = 'C' THEN '整箱出库'
        WHEN carton_type = 'P' THEN '托盘出库'
        ELSE '其他'
    END AS carton_type,
    CASE WHEN carton_type = 'A' THEN 0 ELSE allocate_qty / package_num END as whole_qty,
    CASE WHEN carton_type = 'A' THEN 1 ELSE 0 END as scatter_count,
    CASE WHEN carton_type = 'A' THEN allocate_qty ELSE 0 END as scatter_qty
FROM 
    ods_wms.iwcs_ssc_picking_carton;