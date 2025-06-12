INSERT INTO 
    dwd.logistics_warehouse_iwcs_picking (
        ssc_picking_carton_id,
        create_time,
        pick_time,
        check_time,
        wms_inout_id,
        whole_qty,
        scatter_count
    )
SELECT 
    ssc_picking_carton_id,
    create_date,
    pick_time,
    check_time,
    wms_inout_id,
    CASE WHEN carton_type = 'A' THEN 0 ELSE allocate_qty / package_num END as whole_qty,
    CASE WHEN carton_type = 'A' THEN 1 ELSE 0 END as scatter_count
FROM 
    ods_wms.iwcs_ssc_picking_carton
WHERE 
    create_date >= (CURRENT_DATE() - INTERVAL 60 DAY);