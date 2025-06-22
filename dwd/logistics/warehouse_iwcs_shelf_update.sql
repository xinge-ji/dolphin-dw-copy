TRUNCATE TABLE dwd.logistics_warehouse_iwcs_shelf;
   
INSERT INTO
    dwd.logistics_warehouse_iwcs_shelf (
        ssc_receive_goods_locate_id,
        receive_bill_type,
        com_inv_case_id,
        stock_pos_id,
        create_time,
        goodsownerid,
        goodsowner_name,
        companyid,
        company_name,
        indtlid,
        receiveid,
        scanerid,
        scaner_name,
        ownergoodsid,
        goods_name,
        goods_eng_name,
        goods_type,
        trade_pack_name,
        factory_name,
        product_area,
        lotid,
        lotno,
        ownerpackid,
        pack_name,
        pack_size,
        goods_qty,
        dtl_goods_qty,
        depot_name,
        scatter_count,
        whole_count,
        whole_qty
    )
SELECT
    c.ssc_receive_goods_locate_id,
    a.receive_bill_type,
    g.com_inv_case_id,
    c.stock_pos_id,
    c.create_timestamp AS create_time,
    a.inv_owner AS goodsownerid,
    party_owner.party_name AS goodsowner_name,
    a.send_party_id AS company_id,
    party_company.party_name AS company_name,
    b.external_bill_id AS indtl_id,
    b.external_bill_lines_id AS receive_id,
    g.scaner AS scaner_id,
    i.employeecode AS scaner_name,
    b.com_goods_id AS owner_goods_id,
    f.goods_name AS goods_name,
    f.english_name AS goods_eng_name,
    f.goods_desc AS goods_type,
    goods_package.package_name AS trade_pack_name,
    party_factory.party_name AS factory_name,
    f.product_location AS product_area,
    b.com_lot_id AS lot_id,
    h.lot_no AS lot_no,
    b.package_id AS owner_pack_id,
    b.package_name AS pack_name,
    b.package_num AS pack_size,
    c.locate_qty AS goods_qty,
    g.rec_qty AS dtl_goods_qty,
    e.depot_name,
    IF (e.com_depot_id = 100, 0, 1) AS scatter_count,
    IF (e.com_depot_id = 100, 1, 0) AS whole_count,
    IF (
        e.com_depot_id = 100
        AND g.package_num != 0,
        g.rec_qty / g.package_num,
        0
    ) AS whole_qty
FROM
    ods_wms.iwcs_ssc_receive_goods a
    JOIN ods_wms.iwcs_ssc_receive_goods_lines b ON a.ssc_receive_goods_id = cast(b.ssc_receive_goods_id as decimal(38, 18))
    JOIN ods_wms.iwcs_ssc_receive_goods_locate c ON b.ssc_receive_goods_lines_id = c.ssc_receive_goods_lines_id
    LEFT JOIN ods_wms.iwcs_com_stock_pos d ON c.stock_pos_id = d.stock_pos_id
    LEFT JOIN ods_wms.iwcs_com_depot e ON d.depot_id = e.com_depot_id
    JOIN ods_wms.iwcs_com_goods f ON b.com_goods_id = f.com_goods_id
    JOIN ods_wms.iwcs_com_inv_case_v g ON c.ssc_receive_goods_locate_id = g.ssc_rec_goods_locate_id
    AND c.case_nbr = g.case_nbr
    LEFT JOIN ods_wms.iwcs_com_lot h ON b.com_lot_id = h.com_lot_id
    LEFT JOIN ods_wms.iwcs_sys_userlist i ON g.scaner = i.userid
    -- goodsowner name from com_party type = 1
    LEFT JOIN (
        SELECT
            com_party_id,
            MAX(party_name) AS party_name
        FROM
            ods_wms.iwcs_com_party
        WHERE
            com_party_type_id = 1
        GROUP BY
            com_party_id
    ) party_owner ON party_owner.com_party_id = a.inv_owner
    -- company name from com_party type <> 1
    LEFT JOIN (
        SELECT
            com_party_id,
            MAX(party_name) AS party_name
        FROM
            ods_wms.iwcs_com_party
        WHERE
            com_party_type_id <> 1
        GROUP BY
            com_party_id
    ) party_company ON party_company.com_party_id = a.send_party_id
    -- trade pack name
    LEFT JOIN (
        SELECT
            com_goods_id,
            MAX(package_name) AS package_name
        FROM
            ods_wms.iwcs_com_goods_package
        WHERE
            package_type = 'UNIT'
        GROUP BY
            com_goods_id
    ) goods_package ON goods_package.com_goods_id = b.com_goods_id
    -- factory name from com_party type = 4
    LEFT JOIN (
        SELECT
            com_party_id,
            MAX(party_name) AS party_name
        FROM
            ods_wms.iwcs_com_party
        WHERE
            com_party_type_id = 4
        GROUP BY
            com_party_id
    ) party_factory ON party_factory.com_party_id = f.factory_id;