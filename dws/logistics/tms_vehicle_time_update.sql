INSERT INTO dws.logistics_tms_vehicle_time (
    dispatchid,
    vehicleno,
    goodspeerno,
    goodsowner_name,
    company_name,
    dept_name,
    create_time,
    print_time,
    load_time,
    sign_time
)
SELECT
    a.dispatchid,
    a.vehicleno,
    b.goodspeerno,
    b.goodsowner_name,
    b.company_name,
    c.dept_name,
    a.create_time,
    d.print_time,
    e.load_time,
    e.sign_time
FROM dwd.logistics_tms_dispatch_doc AS a
JOIN dwd.logistics_tms_tr_doc AS b ON a.dispatchid = b.dispatchid
JOIN dwd.logistics_warehouse_order_out_doc AS c ON b.goodspeerno = c.wavedtlid
JOIN dwd.logistics_warehouse_wave_dtl AS d ON c.wavedtlid = d.wavedtlid
JOIN dwd.logistics_tms_order_doc AS e ON b.goodspeerno = e.goodspeerno
WHERE a.warehid = 1
AND b.warehid = 1
AND a.is_print = 1
AND b.sign_time IS NOT NULL
AND c.outmode = '送货';