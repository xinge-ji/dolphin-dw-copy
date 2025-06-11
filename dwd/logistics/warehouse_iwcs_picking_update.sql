INSERT INTO 
    dwd.logistics_warehouse_iwcs_picking (
        ssc_picking_carton_id,
        create_time,
        pick_time,
        check_time,
        wms_inout_id
    )
SELECT 
    ssc_picking_carton_id,
    create_date,
    pick_time,
    check_time,
    wms_inout_id
FROM 
    ods_wms.iwcs_ssc_picking_carton
WHERE 
    create_date >= (CURRENT_DATE() - INTERVAL 60 DAY);