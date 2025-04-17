DROP TABLE IF EXISTS ads.retail_map;

CREATE TABLE ads.retail_map (
    province VARCHAR(100) COMMENT '省份',
    city VARCHAR(100) COMMENT '城市',
    lat DECIMAL(10,6) COMMENT '纬度',
    lng DECIMAL(10,6) COMMENT '经度', 
    medinsName VARCHAR(255) COMMENT '机构名称',
    medinsType VARCHAR(100) COMMENT '机构类型'
) 
UNIQUE KEY (province, city, lat, lng, medinsName) DISTRIBUTED BY HASH (province, city) PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
);

INSERT INTO ads.retail_map (
    province,
    city,
    lat,
    lng,
    medinsName,
    medinsType
)
SELECT 
    province,
    city,
    lat,
    lnt AS lng, 
    medinsName,
    CASE
        WHEN medinsName LIKE '%鹭燕%' THEN '1.鹭燕'
        ELSE '2.零售药店'
    END AS medinsType
FROM ods_yjy.nhsa_lingshouyaodian
WHERE useStatus = '1'

UNION ALL

SELECT
    province,
    city, 
    lat,
    lnt AS lng,
    medinsName,
    CASE
        WHEN medinsTypeName = '普通诊所' THEN '3.普通诊所'
        WHEN medinsTypeName = '中医诊所' THEN '4.中医诊所'
        WHEN medinsTypeName = '综合医院' THEN '5.综合医院'
        WHEN medinsTypeName = '中心卫生院' THEN '6.中心卫生院'
        WHEN medinsTypeName = '社区卫生服务站' THEN '7.社区卫生服务站'
        WHEN medinsTypeName = '中西医结合诊所' THEN '8.中西医结合诊所'
        WHEN medinsTypeName = '社区卫生服务中心' THEN '9.社区卫生服务中心'
        WHEN medinsTypeName = '综合门诊部' THEN '10.综合门诊部'
        WHEN medinsTypeName = '妇幼保健院' THEN '11.妇幼保健院'
        WHEN medinsTypeName = '中医（综合）医院' THEN '12.中医（综合）医院' 
        ELSE '13.其他'
    END AS medinsType
FROM ods_yjy.nhsa_yiliaojigou
WHERE useStatus = '1';