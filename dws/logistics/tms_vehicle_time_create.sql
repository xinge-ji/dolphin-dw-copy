DROP TABLE IF EXISTS dws.logistics_tms_vehicle_time;

CREATE TABLE
    dws.logistics_tms_vehicle_time (
        -- 主键标识
        dispatchid bigint COMMENT '调度单ID',
        vehicleno varchar COMMENT '车辆ID',
        goodspeerno bigint COMMENT '随货同行单号',

        -- 货主
        goodsowner_name varchar COMMENT '货主名称',

        -- 客户
        company_name varchar COMMENT '客户名称',

        -- 业务部门
        dept_name varchar COMMENT '业务部门名称',

        -- 时间
        create_time datetime COMMENT '创建时间',
        print_time datetime COMMENT '打印时间',
        load_time datetime COMMENT '出库时间',
        sign_time datetime COMMENT '签收时间'
    ) UNIQUE KEY (dispatchid, vehicleno, goodspeerno) DISTRIBUTED BY HASH (dispatchid, vehicleno, goodspeerno) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );

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