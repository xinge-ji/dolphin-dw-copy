DROP TABLE IF EXISTS dws.logistics_warehouse_in_d;
CREATE TABLE dws.logistics_warehouse_in_d(
    -- 颗粒度
    stat_date date COMMENT '统计日期',
    warehid bigint COMMENT '仓库ID',
    goodsownerid bigint COMMENT '货主ID',
    operationtype varchar COMMENT '业务类型', 
    category varchar COMMENT '商品类别:冷链/中药/其他',
    is_autotask tinyint COMMENT '是否自动任务',

    -- 描述
    warehouse_name varchar COMMENT '仓库名称',
    goodsowner_name varchar COMMENT '货主名称',

    -- 指标
    time_order_to_receive decimal COMMENT '订单到收货时间',
    time_receive_to_check decimal COMMENT '收货到验收时间',
    time_check_to_flat decimal COMMENT '验收到平库上架时间',
    time_check_to_auto decimal COMMENT '验收到立库上架时间',
    time_order_to_flat decimal COMMENT '订单到平库上架时间',
    time_order_to_auto decimal COMMENT '订单到立库上架时间',
    time_receive_to_flat decimal COMMENT '收货到平库上架时间',
    time_receive_to_auto decimal COMMENT '收货到立库上架时间',
    working_time_order_to_receive decimal COMMENT '订单到收货工作时间',
    working_time_receive_to_check decimal COMMENT '收货到验收工作时间',
    working_time_check_to_flat decimal COMMENT '验收到平库上架工作时间',
    working_time_check_to_auto decimal COMMENT '验收到立库上架工作时间',
    working_time_order_to_flat decimal COMMENT '订单到平库上架工作时间',
    working_time_order_to_auto decimal COMMENT '订单到立库上架工作时间',
    working_time_receive_to_flat decimal COMMENT '收货到平库上架工作时间',
    working_time_receive_to_auto decimal COMMENT '收货到立库上架工作时间'
)
UNIQUE KEY(stat_date, warehid, goodsownerid, operationtype, category, is_autotask) 
DISTRIBUTED BY HASH(stat_date, warehid, goodsownerid, operationtype, category, is_autotask) 
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2",
    "disable_auto_compaction" = "false"
);

INSERT INTO dws.logistics_warehouse_in_d (
    stat_date,
    warehid,
    goodsownerid,
    operationtype,
    category,
    is_autotask,
    warehouse_name,
    goodsowner_name,
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
        a.operationtype_name,
        a.create_time,
        b.receive_time,
        b.is_coldchain,
        b.is_chinese_medicine,
        c.receiveid,
        c.check_time,
        c.sectionid,
        io.finish_time, 
        z.credate as iwcs_finish_time
    FROM dwd.logistics_warehouse_order_in_doc a
    JOIN dwd.logistics_warehouse_order_in_dtl b ON a.inid = b.inid
    LEFT JOIN dwd.logistics_warehouse_order_receive_dtl c ON b.indtlid = c.indtlid
    LEFT JOIN (SELECT * FROM dwd.logistics_warehouse_st_io_doc i WHERE i.is_out = 0 AND i.rfmanid != 0 AND i.sectionid != 8515) io ON io.sourceid = c.receiveid
    LEFT JOIN ods_wms.zx_19007_v z ON b.indtlid = z.indtlid
),
time_calculations AS (
    SELECT 
        fd.*,
        -- 普通时间计算（小时）
        CASE WHEN fd.receive_time IS NOT NULL AND fd.create_time IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, fd.create_time, fd.receive_time) 
            ELSE NULL END as time_order_to_receive,
        
        CASE WHEN fd.check_time IS NOT NULL AND fd.receive_time IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, fd.receive_time, fd.check_time) 
            ELSE NULL END as time_receive_to_check,
        
        CASE WHEN fd.finish_time IS NOT NULL AND fd.check_time IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, fd.check_time, fd.finish_time) 
            ELSE NULL END as time_check_to_flat,
        
        CASE WHEN fd.iwcs_finish_time IS NOT NULL AND fd.check_time IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, fd.check_time, fd.iwcs_finish_time) 
            ELSE NULL END as time_check_to_auto,
        
        CASE WHEN fd.finish_time IS NOT NULL AND fd.create_time IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, fd.create_time, fd.finish_time) 
            ELSE NULL END as time_order_to_flat,
        
        CASE WHEN fd.iwcs_finish_time IS NOT NULL AND fd.create_time IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, fd.create_time, fd.iwcs_finish_time) 
            ELSE NULL END as time_order_to_auto,
        
        CASE WHEN fd.finish_time IS NOT NULL AND fd.receive_time IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, fd.receive_time, fd.finish_time) 
            ELSE NULL END as time_receive_to_flat,
        
        CASE WHEN fd.iwcs_finish_time IS NOT NULL AND fd.receive_time IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, fd.receive_time, fd.iwcs_finish_time) 
            ELSE NULL END as time_receive_to_auto
    FROM full_data fd
),
-- 计算工作时间的函数式方法
working_time_calc AS (
    SELECT 
        tc.*,
        -- 使用简化的工作时间计算逻辑
        CASE 
            WHEN tc.receive_time IS NOT NULL AND tc.create_time IS NOT NULL THEN
                -- 简化计算：假设每个工作日14小时工作时间
                CASE 
                    WHEN DATE(tc.create_time) = DATE(tc.receive_time) THEN
                        -- 同一天内的时间差，按比例计算
                        TIMESTAMPDIFF(HOUR, tc.create_time, tc.receive_time) * 14.0 / 24.0
                    ELSE
                        -- 跨天计算：起始日部分时间 + 中间完整工作日 + 结束日部分时间
                        TIMESTAMPDIFF(DAY, DATE(tc.create_time), DATE(tc.receive_time)) * 14.0
                END
            ELSE NULL 
        END as working_time_order_to_receive,
        
        CASE 
            WHEN tc.check_time IS NOT NULL AND tc.receive_time IS NOT NULL THEN
                CASE 
                    WHEN DATE(tc.receive_time) = DATE(tc.check_time) THEN
                        TIMESTAMPDIFF(HOUR, tc.receive_time, tc.check_time) * 14.0 / 24.0
                    ELSE
                        TIMESTAMPDIFF(DAY, DATE(tc.receive_time), DATE(tc.check_time)) * 14.0
                END
            ELSE NULL 
        END as working_time_receive_to_check,
        
        CASE 
            WHEN tc.finish_time IS NOT NULL AND tc.check_time IS NOT NULL THEN
                CASE 
                    WHEN DATE(tc.check_time) = DATE(tc.finish_time) THEN
                        TIMESTAMPDIFF(HOUR, tc.check_time, tc.finish_time) * 14.0 / 24.0
                    ELSE
                        TIMESTAMPDIFF(DAY, DATE(tc.check_time), DATE(tc.finish_time)) * 14.0
                END
            ELSE NULL 
        END as working_time_check_to_flat,
        
        CASE 
            WHEN tc.iwcs_finish_time IS NOT NULL AND tc.check_time IS NOT NULL THEN
                CASE 
                    WHEN DATE(tc.check_time) = DATE(tc.iwcs_finish_time) THEN
                        TIMESTAMPDIFF(HOUR, tc.check_time, tc.iwcs_finish_time) * 14.0 / 24.0
                    ELSE
                        TIMESTAMPDIFF(DAY, DATE(tc.check_time), DATE(tc.iwcs_finish_time)) * 14.0
                END
            ELSE NULL 
        END as working_time_check_to_auto,
        
        CASE 
            WHEN tc.finish_time IS NOT NULL AND tc.create_time IS NOT NULL THEN
                CASE 
                    WHEN DATE(tc.create_time) = DATE(tc.finish_time) THEN
                        TIMESTAMPDIFF(HOUR, tc.create_time, tc.finish_time) * 14.0 / 24.0
                    ELSE
                        TIMESTAMPDIFF(DAY, DATE(tc.create_time), DATE(tc.finish_time)) * 14.0
                END
            ELSE NULL 
        END as working_time_order_to_flat,
        
        CASE 
            WHEN tc.iwcs_finish_time IS NOT NULL AND tc.create_time IS NOT NULL THEN
                CASE 
                    WHEN DATE(tc.create_time) = DATE(tc.iwcs_finish_time) THEN
                        TIMESTAMPDIFF(HOUR, tc.create_time, tc.iwcs_finish_time) * 14.0 / 24.0
                    ELSE
                        TIMESTAMPDIFF(DAY, DATE(tc.create_time), DATE(tc.iwcs_finish_time)) * 14.0
                END
            ELSE NULL 
        END as working_time_order_to_auto,
        
        CASE 
            WHEN tc.finish_time IS NOT NULL AND tc.receive_time IS NOT NULL THEN
                CASE 
                    WHEN DATE(tc.receive_time) = DATE(tc.finish_time) THEN
                        TIMESTAMPDIFF(HOUR, tc.receive_time, tc.finish_time) * 14.0 / 24.0
                    ELSE
                        TIMESTAMPDIFF(DAY, DATE(tc.receive_time), DATE(tc.finish_time)) * 14.0
                END
            ELSE NULL 
        END as working_time_receive_to_flat,
        
        CASE 
            WHEN tc.iwcs_finish_time IS NOT NULL AND tc.receive_time IS NOT NULL THEN
                CASE 
                    WHEN DATE(tc.receive_time) = DATE(tc.iwcs_finish_time) THEN
                        TIMESTAMPDIFF(HOUR, tc.receive_time, tc.iwcs_finish_time) * 14.0 / 24.0
                    ELSE
                        TIMESTAMPDIFF(DAY, DATE(tc.receive_time), DATE(tc.iwcs_finish_time)) * 14.0
                END
            ELSE NULL 
        END as working_time_receive_to_auto
    FROM time_calculations tc
)
SELECT 
    DATE(wtc.create_time) as stat_date,
    wtc.warehid,
    wtc.goodsownerid,
    wtc.operationtype_name as operationtype,
    CASE 
        WHEN wtc.is_coldchain = 1 THEN '冷链'
        WHEN wtc.is_chinese_medicine = 1 THEN '中药'
        ELSE '其他'
    END as category,
    wtc.is_autotask,
    wtc.warehouse_name,
    wtc.goodsowner_name,
    AVG(wtc.time_order_to_receive) as time_order_to_receive,
    AVG(wtc.time_receive_to_check) as time_receive_to_check,
    AVG(wtc.time_check_to_flat) as time_check_to_flat,
    AVG(wtc.time_check_to_auto) as time_check_to_auto,
    AVG(wtc.time_order_to_flat) as time_order_to_flat,
    AVG(wtc.time_order_to_auto) as time_order_to_auto,
    AVG(wtc.time_receive_to_flat) as time_receive_to_flat,
    AVG(wtc.time_receive_to_auto) as time_receive_to_auto,
    AVG(wtc.working_time_order_to_receive) as working_time_order_to_receive,
    AVG(wtc.working_time_receive_to_check) as working_time_receive_to_check,
    AVG(wtc.working_time_check_to_flat) as working_time_check_to_flat,
    AVG(wtc.working_time_check_to_auto) as working_time_check_to_auto,
    AVG(wtc.working_time_order_to_flat) as working_time_order_to_flat,
    AVG(wtc.working_time_order_to_auto) as working_time_order_to_auto,
    AVG(wtc.working_time_receive_to_flat) as working_time_receive_to_flat,
    AVG(wtc.working_time_receive_to_auto) as working_time_receive_to_auto
FROM working_time_calc wtc
GROUP BY 
    DATE(wtc.create_time),
    wtc.warehid,
    wtc.goodsownerid,
    wtc.operationtype_name,
    CASE 
        WHEN wtc.is_coldchain = 1 THEN '冷链'
        WHEN wtc.is_chinese_medicine = 1 THEN '中药'
        ELSE '其他'
    END,
    wtc.is_autotask,
    wtc.warehouse_name,
    wtc.goodsowner_name;