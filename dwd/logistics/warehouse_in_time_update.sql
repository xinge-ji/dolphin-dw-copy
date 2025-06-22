INSERT INTO dwd.logistics_warehouse_in_time (
    inid,
    indtlid,
    receiveid,
    inoutid,
    ssc_receive_goods_locate_id,
    warehid,
    warehouse_name,
    goodsownerid,
    goodsowner_name,
    is_autotask,
    operation_type,
    create_time,
    receive_time,
    goods_category,
    check_time,
    sectionid,
    finish_time,
    iwcs_finish_time,
    time_order_to_receive,
    time_receive_to_check,
    time_check_to_flat,
    time_check_to_auto,
    time_order_to_flat,
    time_order_to_auto,
    time_receive_to_flat,
    time_receive_to_auto,
    working_time_order_to_receive,
    working_time_receive_to_check,
    working_time_check_to_flat,
    working_time_check_to_auto,
    working_time_order_to_flat,
    working_time_order_to_auto,
    working_time_receive_to_flat,
    working_time_receive_to_auto
)
WITH full_data AS (
    SELECT
        a.inid,
        b.indtlid,
        a.warehid,
        a.warehouse_name,
        a.goodsownerid,
        a.goodsowner_name,
        a.is_autotask,
        a.operation_type,
        a.create_time,
        b.receive_time,
        b.goods_category,
        c.receiveid,
        c.check_time,
        c.sectionid,
        io.finish_time,
        io.inoutid,
        z.credate as iwcs_finish_time,
        z.ssc_receive_goods_locate_id
    FROM dwd.logistics_warehouse_order_in_doc a
    JOIN dwd.logistics_warehouse_order_in_dtl b ON a.inid = b.inid
    LEFT JOIN dwd.logistics_warehouse_order_receive_dtl c ON b.indtlid = c.indtlid
    LEFT JOIN (SELECT * FROM dwd.logistics_warehouse_st_io_doc i WHERE i.is_out = 0 AND i.rfmanid != 0 AND i.sectionid != 8515) io ON io.sourceid = c.receiveid
    LEFT JOIN ods_wms.zx_19007_v z ON b.indtlid = z.indtlid
    WHERE a.create_time >= (CURRENT_DATE() - INTERVAL 90 DAY) 
),
time_calculations AS (
    SELECT 
        fd.*,
        -- 普通时间计算（小时）
        CASE WHEN fd.receive_time IS NOT NULL AND fd.create_time IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, fd.create_time, fd.receive_time) 
            ELSE NULL END as time_order_to_receive,
        
        CASE WHEN fd.check_time IS NOT NULL AND fd.receive_time IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, fd.receive_time, fd.check_time) 
            ELSE NULL END as time_receive_to_check,
        
        CASE WHEN fd.finish_time IS NOT NULL AND fd.check_time IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, fd.check_time, fd.finish_time) 
            ELSE NULL END as time_check_to_flat,
        
        CASE WHEN fd.iwcs_finish_time IS NOT NULL AND fd.check_time IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, fd.check_time, fd.iwcs_finish_time) 
            ELSE NULL END as time_check_to_auto,
        
        CASE WHEN fd.finish_time IS NOT NULL AND fd.create_time IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, fd.create_time, fd.finish_time) 
            ELSE NULL END as time_order_to_flat,
        
        CASE WHEN fd.iwcs_finish_time IS NOT NULL AND fd.create_time IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, fd.create_time, fd.iwcs_finish_time) 
            ELSE NULL END as time_order_to_auto,
        
        CASE WHEN fd.finish_time IS NOT NULL AND fd.receive_time IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, fd.receive_time, fd.finish_time) 
            ELSE NULL END as time_receive_to_flat,
        
        CASE WHEN fd.iwcs_finish_time IS NOT NULL AND fd.receive_time IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, fd.receive_time, fd.iwcs_finish_time) 
            ELSE NULL END as time_receive_to_auto
    FROM full_data fd
),
-- 生成日期范围和工作时间计算
date_ranges AS (
    SELECT 
        tc.inid,
        tc.indtlid,
        tc.receiveid,
        tc.inoutid,
        tc.ssc_receive_goods_locate_id,
        -- 订单到收货日期范围
        CASE WHEN tc.receive_time IS NOT NULL AND tc.create_time IS NOT NULL 
            THEN DATE(tc.create_time) ELSE NULL END as order_start_date,
        CASE WHEN tc.receive_time IS NOT NULL AND tc.create_time IS NOT NULL 
            THEN DATE(tc.receive_time) ELSE NULL END as receive_end_date,
        tc.create_time as order_start_time,
        tc.receive_time as receive_end_time,
        
        -- 收货到验收日期范围
        CASE WHEN tc.check_time IS NOT NULL AND tc.receive_time IS NOT NULL 
            THEN DATE(tc.receive_time) ELSE NULL END as receive_start_date,
        CASE WHEN tc.check_time IS NOT NULL AND tc.receive_time IS NOT NULL 
            THEN DATE(tc.check_time) ELSE NULL END as check_end_date,
        tc.receive_time as receive_start_time,
        tc.check_time as check_end_time,
        
        -- 验收到平库日期范围
        CASE WHEN tc.finish_time IS NOT NULL AND tc.check_time IS NOT NULL 
            THEN DATE(tc.check_time) ELSE NULL END as check_start_date,
        CASE WHEN tc.finish_time IS NOT NULL AND tc.check_time IS NOT NULL 
            THEN DATE(tc.finish_time) ELSE NULL END as flat_end_date,
        tc.check_time as check_start_time,
        tc.finish_time as flat_end_time,
        
        -- 验收到立库日期范围
        CASE WHEN tc.iwcs_finish_time IS NOT NULL AND tc.check_time IS NOT NULL 
            THEN DATE(tc.check_time) ELSE NULL END as check_start_date2,
        CASE WHEN tc.iwcs_finish_time IS NOT NULL AND tc.check_time IS NOT NULL 
            THEN DATE(tc.iwcs_finish_time) ELSE NULL END as auto_end_date,
        tc.check_time as check_start_time2,
        tc.iwcs_finish_time as auto_end_time
    FROM time_calculations tc
),
-- 计算各个时间段的工作时间
working_time_order_to_receive AS (
    SELECT 
        dr.inid,
        dr.indtlid,
        SUM(
            CASE 
                -- 开始和结束在同一工作日
                WHEN dr.order_start_date = dr.receive_end_date AND d.date_key = dr.order_start_date THEN
                    GREATEST(0, 
                        TIMESTAMPDIFF(MINUTE, 
                            GREATEST(dr.order_start_time, STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s')),
                            LEAST(dr.receive_end_time, STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s'))
                        )
                    )
                -- 开始日期
                WHEN d.date_key = dr.order_start_date THEN
                    GREATEST(0,
                        TIMESTAMPDIFF(MINUTE,
                            GREATEST(dr.order_start_time, STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s')),
                            STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s')
                        )
                    )
                -- 结束日期
                WHEN d.date_key = dr.receive_end_date THEN
                    GREATEST(0,
                        TIMESTAMPDIFF(MINUTE,
                            STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s'),
                            LEAST(dr.receive_end_time, STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s'))
                        )
                    )
                -- 中间完整工作日
                WHEN d.date_key > dr.order_start_date AND d.date_key < dr.receive_end_date THEN
                    14.0 * 60
                ELSE 0
            END
        ) as working_time_order_to_receive
    FROM date_ranges dr
    LEFT JOIN dim.date d ON d.date_key >= dr.order_start_date 
                         AND d.date_key <= dr.receive_end_date
    WHERE dr.order_start_date IS NOT NULL AND dr.receive_end_date IS NOT NULL
    GROUP BY dr.inid, dr.indtlid
),
working_time_receive_to_check AS (
    SELECT 
        dr.inid,
        dr.indtlid,
        dr.receiveid,
        SUM(
            CASE 
                WHEN dr.receive_start_date = dr.check_end_date AND d.date_key = dr.receive_start_date THEN
                    GREATEST(0, 
                        TIMESTAMPDIFF(MINUTE, 
                            GREATEST(dr.receive_start_time, STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s')),
                            LEAST(dr.check_end_time, STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s'))
                        )
                    )
                WHEN d.date_key = dr.receive_start_date THEN
                    GREATEST(0,
                        TIMESTAMPDIFF(MINUTE,
                            GREATEST(dr.receive_start_time, STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s')),
                            STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s')
                        )
                    )
                WHEN d.date_key = dr.check_end_date THEN
                    GREATEST(0,
                        TIMESTAMPDIFF(MINUTE,
                            STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s'),
                            LEAST(dr.check_end_time, STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s'))
                        )
                    )
                WHEN d.date_key > dr.receive_start_date AND d.date_key < dr.check_end_date THEN
                    14.0 * 60
                ELSE 0
            END
        ) as working_time_receive_to_check
    FROM date_ranges dr
    LEFT JOIN dim.date d ON d.date_key >= dr.receive_start_date 
                         AND d.date_key <= dr.check_end_date
    WHERE dr.receive_start_date IS NOT NULL AND dr.check_end_date IS NOT NULL
    GROUP BY dr.inid, dr.indtlid, dr.receiveid
),
working_time_check_to_flat AS (
    SELECT 
        dr.inid,
        dr.indtlid,
        dr.receiveid,
        dr.inoutid,
        SUM(
            CASE 
                WHEN dr.check_start_date = dr.flat_end_date AND d.date_key = dr.check_start_date THEN
                    GREATEST(0, 
                        TIMESTAMPDIFF(MINUTE, 
                            GREATEST(dr.check_start_time, STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s')),
                            LEAST(dr.flat_end_time, STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s'))
                        )
                    ) 
                WHEN d.date_key = dr.check_start_date THEN
                    GREATEST(0,
                        TIMESTAMPDIFF(MINUTE,
                            GREATEST(dr.check_start_time, STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s')),
                            STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s')
                        )
                    ) 
                WHEN d.date_key = dr.flat_end_date THEN
                    GREATEST(0,
                        TIMESTAMPDIFF(MINUTE,
                            STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s'),
                            LEAST(dr.flat_end_time, STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s'))
                        )
                    ) 
                WHEN d.date_key > dr.check_start_date AND d.date_key < dr.flat_end_date THEN
                    14.0 * 60
                ELSE 0
            END
        ) as working_time_check_to_flat
    FROM date_ranges dr
    LEFT JOIN dim.date d ON d.date_key >= dr.check_start_date 
                         AND d.date_key <= dr.flat_end_date
    WHERE dr.check_start_date IS NOT NULL AND dr.flat_end_date IS NOT NULL
    GROUP BY dr.inid, dr.indtlid, dr.receiveid, dr.inoutid
),
working_time_check_to_auto AS (
    SELECT 
        dr.inid,
        dr.indtlid,
        dr.receiveid,
        dr.ssc_receive_goods_locate_id,
        SUM(
            CASE 
                WHEN dr.check_start_date2 = dr.auto_end_date AND d.date_key = dr.check_start_date2 THEN
                    GREATEST(0, 
                        TIMESTAMPDIFF(MINUTE, 
                            GREATEST(dr.check_start_time2, STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s')),
                            LEAST(dr.auto_end_time, STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s'))
                        )
                    ) 
                WHEN d.date_key = dr.check_start_date2 THEN
                    GREATEST(0,
                        TIMESTAMPDIFF(MINUTE,
                            GREATEST(dr.check_start_time2, STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s')),
                            STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s')
                        )
                    ) 
                WHEN d.date_key = dr.auto_end_date THEN
                    GREATEST(0,
                        TIMESTAMPDIFF(MINUTE,
                            STR_TO_DATE(CONCAT(d.date_key, ' 08:00:00'), '%Y-%m-%d %H:%i:%s'),
                            LEAST(dr.auto_end_time, STR_TO_DATE(CONCAT(d.date_key, ' 21:59:59'), '%Y-%m-%d %H:%i:%s'))
                        )
                    ) 
                WHEN d.date_key > dr.check_start_date2 AND d.date_key < dr.auto_end_date THEN
                    14.0 * 60
                ELSE 0
            END
        ) as working_time_check_to_auto
    FROM date_ranges dr
    LEFT JOIN dim.date d ON d.date_key >= dr.check_start_date2 
                         AND d.date_key <= dr.auto_end_date
    WHERE dr.check_start_date2 IS NOT NULL AND dr.auto_end_date IS NOT NULL
    GROUP BY dr.inid, dr.indtlid, dr.receiveid, dr.ssc_receive_goods_locate_id
)
SELECT 
    tc.inid,
    tc.indtlid,
    tc.receiveid,
    tc.inoutid,
    tc.ssc_receive_goods_locate_id,
    tc.warehid,
    tc.warehouse_name,
    tc.goodsownerid,
    tc.goodsowner_name,
    tc.is_autotask,
    tc.operation_type,
    tc.create_time,
    tc.receive_time,
    tc.goods_category,
    tc.check_time,
    tc.sectionid,
    tc.finish_time,
    tc.iwcs_finish_time,
    tc.time_order_to_receive,
    tc.time_receive_to_check,
    tc.time_check_to_flat,
    tc.time_check_to_auto,
    tc.time_order_to_flat,
    tc.time_order_to_auto,
    tc.time_receive_to_flat,
    tc.time_receive_to_auto,
    COALESCE(wtor.working_time_order_to_receive, 0) as working_time_order_to_receive,
    COALESCE(wrtc.working_time_receive_to_check, 0) as working_time_receive_to_check,
    wctf.working_time_check_to_flat as working_time_check_to_flat,
    wcta.working_time_check_to_auto as working_time_check_to_auto,
    -- 组合计算的工作时间
    CASE WHEN wctf.working_time_check_to_flat is not NULL THEN COALESCE(wtor.working_time_order_to_receive, 0) + COALESCE(wrtc.working_time_receive_to_check, 0) + wctf.working_time_check_to_flat ELSE NULL END as working_time_order_to_flat,
    CASE WHEN wcta.working_time_check_to_auto is not NULL THEN COALESCE(wtor.working_time_order_to_receive, 0) + COALESCE(wrtc.working_time_receive_to_check, 0) + wcta.working_time_check_to_auto ELSE NULL END as working_time_order_to_auto,
    CASE WHEN wctf.working_time_check_to_flat is not NULL THEN COALESCE(wrtc.working_time_receive_to_check, 0) + wctf.working_time_check_to_flat ELSE NULL END as working_time_receive_to_flat,
    CASE WHEN wcta.working_time_check_to_auto is not NULL THEN COALESCE(wrtc.working_time_receive_to_check, 0) + wcta.working_time_check_to_auto ELSE NULL END as working_time_receive_to_auto
FROM time_calculations tc
LEFT JOIN working_time_order_to_receive wtor ON tc.inid = wtor.inid AND tc.indtlid = wtor.indtlid
LEFT JOIN working_time_receive_to_check wrtc ON tc.inid = wrtc.inid AND tc.indtlid = wrtc.indtlid AND tc.receiveid = wrtc.receiveid
LEFT JOIN working_time_check_to_flat wctf ON tc.inid = wctf.inid AND tc.indtlid = wctf.indtlid AND tc.receiveid = wctf.receiveid AND tc.inoutid = wctf.inoutid 
LEFT JOIN working_time_check_to_auto wcta ON tc.inid = wcta.inid AND tc.indtlid = wcta.indtlid AND tc.receiveid = wcta.receiveid AND tc.ssc_receive_goods_locate_id = wcta.ssc_receive_goods_locate_id;