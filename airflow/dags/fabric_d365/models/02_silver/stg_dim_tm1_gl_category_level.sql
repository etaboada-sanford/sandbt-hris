/* TODO: Move to a separate TM1 project */

with ct as (
    select
        {{ dbt_utils.generate_surrogate_key(['recid']) }} as dim_tm1_gl_category_level_sk
        , recid
        , mainaccountid
        , gl_calc_factor
        , gl_category_l0
        , gl_category_l1
        , gl_category_l2
        , gl_category_l3
        , gl_category_l4
        , gl_category_l5
        , gl_category_l6
        , gl_category_l7
        , gl_category_l8
        , gl_category_l9
        , gl_category_l10
        , gl_category_l11
        , gl_category_l12
        , gl_category_l13
    from (
        values
        (
            1
            , '601000'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Export Sales FOB'
            , 'Total Export CIF'
            , 'Export Product Sales'
        )
        , (
            2
            , '601100'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Export Sales FOB'
            , 'Total Export CIF'
            , 'Commissions, Claims & Discounts - Export'
        )
        , (
            3
            , '601101'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Export Sales FOB'
            , 'Total Export CIF'
            , 'Commissions, Claims & Discounts - Export'
        )
        , (
            4
            , '601102'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Export Sales FOB'
            , 'Total Export CIF'
            , 'Commissions, Claims & Discounts - Export'
        )
        , (
            5
            , '601103'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Export Sales FOB'
            , 'Total Export CIF'
            , 'Commissions, Claims & Discounts - Export'
        )
        , (
            6
            , '601200'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Export Sales FOB'
            , ''
            , 'Total Export Freight'
        )
        , (
            7
            , '601201'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Export Sales FOB'
            , ''
            , 'Total Export Freight'
        )
        , (
            8
            , '602102'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Local Sales'
            , 'Local Sales'
            , 'Accrued Sales - Local'
        )
        , (
            9
            , '602100'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Local Sales'
            , 'Local Sales'
            , 'Commissions, Claims & Discounts - Local'
        )
        , (
            10
            , '602101'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Local Sales'
            , 'Local Sales'
            , 'Commissions, Claims & Discounts - Local'
        )
        , (
            11
            , '602104'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Local Sales'
            , 'Local Sales'
            , 'Commissions, Claims & Discounts - Local'
        )
        , (
            12
            , '602000'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Local Sales'
            , 'Local Sales'
            , 'Local Product Sales'
        )
        , (
            13
            , '602200'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Local Sales'
            , 'Local Freight'
            , 'Local Freight'
        )
        , (
            14
            , '602201'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Seadfood Sales (Exc Interco)'
            , 'Total Local Sales'
            , 'Local Freight'
            , 'Local Freight'
        )
        , (
            15
            , '700100'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Standard Cost of Goods Sold'
            , ''
            , ''
            , 'Cost of Goods Sold at Std (Excl Interco)'
        )
        , (
            16
            , '700101'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Standard Cost of Goods Sold'
            , ''
            , ''
            , 'Cost of Goods Sold at Std (Excl Interco)'
        )
        , (
            17
            , '700103'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Standard Cost of Goods Sold'
            , ''
            , ''
            , 'Cost of Goods Sold at Std (Excl Interco)'
        )
        , (
            18
            , '700104'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Standard Cost of Goods Sold'
            , ''
            , ''
            , 'Cost of Goods Sold at Std (Excl Interco)'
        )
        , (
            19
            , '700102'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Gross Margin at Standard'
            , 'Total Standard Cost of Goods Sold'
            , ''
            , ''
            , 'Cost of Goods Sold at Std (Interco)'
        )
        , (
            20
            , '611300'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Other Income'
            , 'Commission Received'
        )
        , (
            21
            , '611301'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Other Income'
            , 'Commission Received'
        )
        , (
            22
            , '611200'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Other Income'
            , 'Miscellaneous Charges'
        )
        , (
            23
            , '611400'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Other Income'
            , 'Other Sales'
        )
        , (
            24
            , '611100'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Other Income'
            , 'Rental Income'
        )
        , (
            25
            , '611101'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Other Income'
            , 'Rental Income'
        )
        , (
            26
            , '602103'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Fish Processing Fee'
        )
        , (
            27
            , '611000'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Retail Sales'
        )
        , (
            28
            , '611010'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Services Income'
        )
        , (
            29
            , '611500'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Other Services & Hiring Income'
        )
        , (
            30
            , '611501'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Other Services & Hiring Income'
        )
        , (
            31
            , '611502'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Other Services & Hiring Income'
        )
        , (
            32
            , '611503'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Other Services & Hiring Income'
        )
        , (
            33
            , '611504'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Other Services & Hiring Income'
        )
        , (
            34
            , '611505'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Other Services & Hiring Income'
        )
        , (
            35
            , '612000'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Total ACE Trading'
        )
        , (
            36
            , '727500'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Management Fees'
        )
        , (
            37
            , '727501'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Management Fees'
        )
        , (
            38
            , '727502'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Sundry & Deferred Income'
        )
        , (
            39
            , '727503'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Other Income'
            , ''
            , ''
            , 'Total Non-Seafood Sales'
            , 'Sundry & Deferred Income'
        )
        , (
            40
            , '613000'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , ''
            , 'Intercompany Seafood Sales'
        )
        , (
            41
            , '613001'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , ''
            , 'Intercompany Seafood Sales'
        )
        , (
            42
            , '613002'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , ''
            , 'Intercompany Seafood Sales'
        )
        , (
            43
            , '613003'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , ''
            , 'Intercompany Seafood Sales'
        )
        , (
            44
            , '613004'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , ''
            , 'Intercompany Seafood Sales'
        )
        , (
            45
            , '613005'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , ''
            , 'Intercompany Seafood Sales'
        )
        , (
            46
            , '613200'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Internal Seafood Sales'
            , 'Internal Sales'
        )
        , (
            47
            , '613105'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Royalties'
        )
        , (
            48
            , '613300'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Management Fees Received'
        )
        , (
            49
            , '613301'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Management Fees Received'
        )
        , (
            50
            , '613302'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Management Fees Received'
        )
        , (
            51
            , '613303'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Management Fees Received'
        )
        , (
            52
            , '613304'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Management Fees Received'
        )
        , (
            53
            , '613400'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Sundry Income'
        )
        , (
            54
            , '613401'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Sundry Income'
        )
        , (
            55
            , '613402'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Sundry Income'
        )
        , (
            56
            , '613403'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Sundry Income'
        )
        , (
            57
            , '613404'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Sundry Income'
        )
        , (
            58
            , '613500'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Rent Received'
        )
        , (
            59
            , '613501'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Rent Received'
        )
        , (
            60
            , '613502'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Rent Received'
        )
        , (
            61
            , '613503'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Rent Received'
        )
        , (
            62
            , '613504'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Rent Received'
        )
        , (
            63
            , '613600'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Rent Received'
        )
        , (
            64
            , '613602'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Intercompany Sales'
            , ''
            , ''
            , 'Intercompany Non-Seafood Sales'
            , 'Intercompany Rent Received'
        )
        , (
            65
            , '701100'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Total Material and Packaging Absorbed at Standard'
            , 'Material and Packaging Recovery'
        )
        , (
            66
            , '701200'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Total Labour Absorbed at Standard Cost'
            , 'Labour Recovery'
        )
        , (
            67
            , '701201'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Total Labour Absorbed at Standard Cost'
            , 'Labour Recovery'
        )
        , (
            68
            , '701300'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Overhead Recovery'
            , 'Overhead Recovery'
        )
        , (
            69
            , '701301'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Overhead Recovery'
            , 'Overhead Recovery'
        )
        , (
            70
            , '701302'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Overhead Recovery'
            , 'Overhead Recovery'
        )
        , (
            71
            , '701303'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Overhead Recovery'
            , 'Overhead Recovery'
        )
        , (
            72
            , '701304'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Overhead Recovery'
            , 'Overhead Recovery'
        )
        , (
            73
            , '701305'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Overhead Recovery'
            , 'Overhead Recovery'
        )
        , (
            74
            , '701306'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Overhead Recovery'
            , 'Overhead Recovery'
        )
        , (
            75
            , '701400'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Operating Recoveries'
            , 'Inbound Logistics Recovery'
            , 'Inbound Logistics Recovery'
        )
        , (
            76
            , '701500'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Recoveries'
            , 'Stock On Board'
            , ''
            , 'Stock On Board'
        )
        , (
            77
            , '702003'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'External Material Cost'
            , 'Purchase Price Variance'
        )
        , (
            78
            , '702000'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'External Material Cost'
            , 'Raw Material Purchase'
        )
        , (
            79
            , '702001'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'External Material Cost'
            , 'Raw Material Purchase'
        )
        , (
            80
            , '702002'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'External Material Cost'
            , 'Raw Material Purchase'
        )
        , (
            81
            , '702150'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Intercompany Material Cost'
            , 'Internal Processing Fee'
        )
        , (
            82
            , '702100'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Intercompany Material Cost'
            , 'Internal Purchases'
        )
        , (
            83
            , '702101'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Intercompany Material Cost'
            , 'Internal Purchases'
        )
        , (
            84
            , '702102'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Intercompany Material Cost'
            , 'Internal Purchases'
        )
        , (
            85
            , '702103'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Intercompany Material Cost'
            , 'Internal Purchases'
        )
        , (
            86
            , '702104'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Intercompany Material Cost'
            , 'Internal Purchases'
        )
        , (
            87
            , '702105'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Intercompany Material Cost'
            , 'Internal Purchases'
        )
        , (
            88
            , '702200'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Material Cost Issues to Production'
            , 'Material Issues'
        )
        , (
            89
            , '702201'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Material Cost Issues to Production'
            , 'Material Variances'
        )
        , (
            90
            , '702202'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Material Cost Issues to Production'
            , 'Material Variances'
        )
        , (
            91
            , '702203'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Material Cost Issues to Production'
            , 'Material Variances'
        )
        , (
            92
            , '702204'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Material Costs'
            , 'Material Cost Issues to Production'
            , 'Material Variances'
        )
        , (
            93
            , '703000'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Packaging Costs'
            , ''
            , 'Packaging Cost'
        )
        , (
            94
            , '703001'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Packaging Costs'
            , ''
            , 'Packaging Cost'
        )
        , (
            95
            , '703002'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Packaging Costs'
            , ''
            , 'Packaging Cost'
        )
        , (
            96
            , '703003'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Packaging Costs'
            , ''
            , 'Packaging Cost'
        )
        , (
            97
            , '703004'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Packaging Costs'
            , ''
            , 'Packaging Cost'
        )
        , (
            98
            , '703005'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Packaging Costs'
            , ''
            , 'Packaging Variances'
        )
        , (
            99
            , '703006'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Packaging Costs'
            , ''
            , 'Packaging Variances'
        )
        , (
            100
            , '703007'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Packaging Costs'
            , ''
            , 'Packaging Variances'
        )
        , (
            101
            , '703008'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Packaging Costs'
            , ''
            , 'Packaging Variances'
        )
        , (
            102
            , '704000'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Feed Cost'
            , ''
            , 'Feed Cost'
        )
        , (
            103
            , '704001'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Feed Cost'
            , ''
            , 'Feed Cost'
        )
        , (
            104
            , '704002'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Feed Cost'
            , ''
            , 'Feed Cost'
        )
        , (
            105
            , '704003'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Feed Cost'
            , ''
            , 'Feed Cost'
        )
        , (
            106
            , '720900'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Catching and Charter Fees'
            , ''
            , 'Catching and Charter Fees'
        )
        , (
            107
            , '720901'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Catching and Charter Fees'
            , ''
            , 'Catching and Charter Fees'
        )
        , (
            108
            , '710000'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Wages'
        )
        , (
            109
            , '710001'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Wages'
        )
        , (
            110
            , '710002'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Wages'
        )
        , (
            111
            , '710003'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Wages'
        )
        , (
            112
            , '710004'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Wages'
        )
        , (
            113
            , '710005'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Wages'
        )
        , (
            114
            , '710006'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Wages'
        )
        , (
            115
            , '710007'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Wages'
        )
        , (
            116
            , '710008'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Wages'
        )
        , (
            117
            , '710009'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Wages'
        )
        , (
            118
            , '710100'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Temporary Labour'
        )
        , (
            119
            , '710101'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Temporary Labour'
        )
        , (
            120
            , '710102'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Temporary Labour'
        )
        , (
            121
            , '710200'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Crew Cost'
        )
        , (
            122
            , '710300'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Labour Variances'
        )
        , (
            123
            , '710301'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Labour Variances'
        )
        , (
            124
            , '710302'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Labour Variances'
        )
        , (
            125
            , '710303'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Total Labour Costs'
            , ''
            , 'Labour Variances'
        )
        , (
            126
            , '720000'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Commissions and Profit Share'
        )
        , (
            127
            , '720001'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Commissions and Profit Share'
        )
        , (
            128
            , '721500'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Compliance and Inspection'
        )
        , (
            129
            , '721501'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Compliance and Inspection'
        )
        , (
            130
            , '721502'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Compliance and Inspection'
        )
        , (
            131
            , '721503'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Compliance and Inspection'
        )
        , (
            132
            , '721504'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Compliance and Inspection'
        )
        , (
            133
            , '721505'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Compliance and Inspection'
        )
        , (
            134
            , '721600'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Harvesting Costs'
        )
        , (
            135
            , '721700'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Consumables'
        )
        , (
            136
            , '721701'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Consumables'
        )
        , (
            137
            , '721702'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Consumables'
        )
        , (
            138
            , '721703'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Consumables'
        )
        , (
            139
            , '721704'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Consumables'
        )
        , (
            140
            , '721705'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Consumables'
        )
        , (
            141
            , '721706'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Consumables'
        )
        , (
            142
            , '721707'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Consumables'
        )
        , (
            143
            , '721800'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Cleaning'
        )
        , (
            144
            , '722000'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Water, Electricity and Gas'
        )
        , (
            145
            , '722001'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Water, Electricity and Gas'
        )
        , (
            146
            , '722002'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Water, Electricity and Gas'
        )
        , (
            147
            , '722100'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Ice and Brine'
        )
        , (
            148
            , '722101'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Ice and Brine'
        )
        , (
            149
            , '722102'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Ice and Brine'
        )
        , (
            150
            , '722500'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Waste Disposal'
        )
        , (
            151
            , '722800'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Canteen and Food Stores'
        )
        , (
            152
            , '722801'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Canteen and Food Stores'
        )
        , (
            153
            , '722802'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Canteen and Food Stores'
        )
        , (
            154
            , '722803'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Canteen and Food Stores'
        )
        , (
            155
            , '724300'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Subcontracting issued to Production at Standard'
        )
        , (
            156
            , '724400'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Overhead consumables'
        )
        , (
            157
            , '724401'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Overhead consumables'
        )
        , (
            158
            , '720300'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Employee Benefits'
        )
        , (
            159
            , '720301'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Employee Benefits'
        )
        , (
            160
            , '720302'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Employee Benefits'
        )
        , (
            161
            , '720303'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Employee Benefits'
        )
        , (
            162
            , '720304'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Employee Benefits'
        )
        , (
            163
            , '720305'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Employee Benefits'
        )
        , (
            164
            , '720306'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Employee Benefits'
        )
        , (
            165
            , '720307'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Employee Benefits'
        )
        , (
            166
            , '720308'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Employee Benefits'
        )
        , (
            167
            , '724500'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Overhead Variances'
        )
        , (
            168
            , '724501'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Overhead Variances'
        )
        , (
            169
            , '724502'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Overhead Variances'
        )
        , (
            170
            , '724503'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Overhead Variances'
        )
        , (
            171
            , '720800'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Processing Fees'
        )
        , (
            172
            , '721000'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Spotting Fees'
        )
        , (
            173
            , '721001'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Spotting Fees'
        )
        , (
            174
            , '721100'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Fuel and Oil'
        )
        , (
            175
            , '721101'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Fuel and Oil'
        )
        , (
            176
            , '721102'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Fuel and Oil'
        )
        , (
            177
            , '721103'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Fuel and Oil'
        )
        , (
            178
            , '721200'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Gear and Nets'
        )
        , (
            179
            , '721201'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Gear and Nets'
        )
        , (
            180
            , '721300'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'Wharfage'
        )
        , (
            181
            , '721400'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'ACE In, Levies and Deemed Values'
        )
        , (
            182
            , '721401'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'ACE In, Levies and Deemed Values'
        )
        , (
            183
            , '721402'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'ACE In, Levies and Deemed Values'
        )
        , (
            184
            , '721403'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'ACE In, Levies and Deemed Values'
        )
        , (
            185
            , '721404'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'ACE In, Levies and Deemed Values'
        )
        , (
            186
            , '721405'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'ACE In, Levies and Deemed Values'
        )
        , (
            187
            , '721406'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'ACE In, Levies and Deemed Values'
        )
        , (
            188
            , '721407'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Variable Overheads excl Catch Fees'
            , ''
            , 'ACE In, Levies and Deemed Values'
        )
        , (
            189
            , '720100'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Asset Maintenance and Repairs'
        )
        , (
            190
            , '720200'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Asset Maintenance and Repairs'
        )
        , (
            191
            , '722900'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Insurance'
        )
        , (
            192
            , '720400'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'ACC and Medical Costs'
        )
        , (
            193
            , '720401'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'ACC and Medical Costs'
        )
        , (
            194
            , '720402'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'ACC and Medical Costs'
        )
        , (
            195
            , '720403'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'ACC and Medical Costs'
        )
        , (
            196
            , '720404'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'ACC and Medical Costs'
        )
        , (
            197
            , '720500'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Salaries'
        )
        , (
            198
            , '720600'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Hiring and Recruitment'
        )
        , (
            199
            , '720601'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Hiring and Recruitment'
        )
        , (
            200
            , '720602'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Hiring and Recruitment'
        )
        , (
            201
            , '720603'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Hiring and Recruitment'
        )
        , (
            202
            , '720604'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Hiring and Recruitment'
        )
        , (
            203
            , '720700'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Travel and Entertainment'
        )
        , (
            204
            , '720701'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Travel and Entertainment'
        )
        , (
            205
            , '720702'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Travel and Entertainment'
        )
        , (
            206
            , '720703'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Travel and Entertainment'
        )
        , (
            207
            , '720704'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Travel and Entertainment'
        )
        , (
            208
            , '720705'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Travel and Entertainment'
        )
        , (
            209
            , '720706'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Travel and Entertainment'
        )
        , (
            210
            , '721900'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            211
            , '721901'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            212
            , '721902'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            213
            , '721903'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            214
            , '721904'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            215
            , '721905'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            216
            , '721906'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            217
            , '721907'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            218
            , '721908'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            219
            , '721909'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            220
            , '721910'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Depreciation'
        )
        , (
            221
            , '722200'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Rent and Equipment Hire'
        )
        , (
            222
            , '722201'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Rent and Equipment Hire'
        )
        , (
            223
            , '722202'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Rent and Equipment Hire'
        )
        , (
            224
            , '722203'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Rent and Equipment Hire'
        )
        , (
            225
            , '722204'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Rent and Equipment Hire'
        )
        , (
            226
            , '722205'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Rent and Equipment Hire'
        )
        , (
            227
            , '722300'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Vessel Management'
        )
        , (
            228
            , '722400'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Health and Safety'
        )
        , (
            229
            , '722401'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Health and Safety'
        )
        , (
            230
            , '722402'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Health and Safety'
        )
        , (
            231
            , '722403'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Fixed Overheads'
            , ''
            , 'Health and Safety'
        )
        , (
            232
            , '725000'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Inbound Logistics Cost'
            , ''
            , 'Inbound Logistics Cost'
        )
        , (
            233
            , '725001'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Inbound Logistics Cost'
            , ''
            , 'Inbound Logistics Cost'
        )
        , (
            234
            , '725002'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Inbound Logistics Cost'
            , ''
            , 'Inbound Logistics Cost'
        )
        , (
            235
            , '725003'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Inbound Logistics Cost'
            , ''
            , 'Inbound Logistics Cost'
        )
        , (
            236
            , '725004'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Total Operating Variance'
            , 'Total Operating Expenses'
            , 'Inbound Logistics Cost'
            , ''
            , 'Inbound Logistics Cost'
        )
        , (
            237
            , '726200'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Internal Sales and Purchases'
        )
        , (
            238
            , '726201'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Internal Sales and Purchases'
        )
        , (
            239
            , '726202'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Internal Sales and Purchases'
        )
        , (
            240
            , '726203'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Internal Sales and Purchases'
        )
        , (
            241
            , '726204'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Internal Sales and Purchases'
        )
        , (
            242
            , '726205'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Internal Sales and Purchases'
        )
        , (
            243
            , '726206'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Internal Sales and Purchases'
        )
        , (
            244
            , '726207'
            , -1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Internal Sales and Purchases'
        )
        , (
            245
            , '726208'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Internal Sales and Purchases'
        )
        , (
            246
            , '726300'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Management Fees Paid'
        )
        , (
            247
            , '726301'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Management Fees Paid'
        )
        , (
            248
            , '726302'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Management Fees Paid'
        )
        , (
            249
            , '726303'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Management Fees Paid'
        )
        , (
            250
            , '726304'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Management Fees Paid'
        )
        , (
            251
            , '726400'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Sundry Expense'
        )
        , (
            252
            , '726401'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Sundry Expense'
        )
        , (
            253
            , '726402'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Sundry Expense'
        )
        , (
            254
            , '726403'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Sundry Expense'
        )
        , (
            255
            , '726404'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Sundry Expense'
        )
        , (
            256
            , '726405'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Sundry Expense'
        )
        , (
            257
            , '726500'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Rent'
        )
        , (
            258
            , '726501'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Rent'
        )
        , (
            259
            , '726502'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Rent'
        )
        , (
            260
            , '726503'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Rent'
        )
        , (
            261
            , '726504'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Rent'
        )
        , (
            262
            , '726505'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Rent'
        )
        , (
            263
            , '726600'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Harvesting Expense'
        )
        , (
            264
            , '726602'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Harvesting Expense'
        )
        , (
            265
            , '726605'
            , 1
            , 'Profit and Loss'
            , 'Total Profit and Loss'
            , 'Profit After Tax'
            , 'Profit Before Tax'
            , 'EBIT'
            , 'Normalised EBIT'
            , 'Net Operating Profit'
            , 'Gross Margin After Biomass'
            , 'Gross Margin Before Biomass'
            , 'Net Intracompany'
            , ''
            , ''
            , ''
            , 'Intercompany Harvesting Expense'
        )
        , (266, '726000', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Gross Margin After Biomass', 'Biomass Movement', '', '', '', '', 'Biomass Movement')
        , (267, '726001', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Gross Margin After Biomass', 'Biomass Movement', '', '', '', '', 'Biomass Movement')
        , (268, '726002', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Gross Margin After Biomass', 'Biomass Movement', '', '', '', '', 'Biomass Movement')
        , (269, '722600', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Rates')
        , (270, '723700', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Directors Fees')
        , (271, '723800', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Sponsorships and Donations')
        , (272, '723801', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Sponsorships and Donations')
        , (273, '723802', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Sponsorships and Donations')
        , (274, '723803', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Sponsorships and Donations')
        , (275, '723900', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Research')
        , (276, '724000', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Shareholder Expenses')
        , (277, '724100', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Bank Charges')
        , (278, '724101', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Bank Charges')
        , (279, '724200', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Overhead Transfer to COGS')
        , (280, '724201', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Overhead Transfer to COGS')
        , (281, '724202', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Overhead Transfer to COGS')
        , (282, '724203', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Overhead Transfer to COGS')
        , (283, '722700', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Security')
        , (284, '723000', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Legal and Consulting Expenses')
        , (285, '723001', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Legal and Consulting Expenses')
        , (286, '723002', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Legal and Consulting Expenses')
        , (287, '723003', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Legal and Consulting Expenses')
        , (288, '723004', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Legal and Consulting Expenses')
        , (289, '723100', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Office and Communication Expenses')
        , (290, '723101', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Office and Communication Expenses')
        , (291, '723200', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Subscriptions')
        , (292, '723300', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Audit Fees')
        , (293, '723301', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Audit Fees')
        , (294, '723302', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Audit Fees')
        , (295, '723303', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Audit Fees')
        , (296, '723304', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Audit Fees')
        , (297, '723400', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Marketing')
        , (298, '723500', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Bad Debts')
        , (299, '723501', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Bad Debts')
        , (300, '723502', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'Bad Debts')
        , (301, '723600', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'IT Costs')
        , (302, '723601', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'IT Costs')
        , (303, '723602', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'IT Costs')
        , (304, '723603', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'IT Costs')
        , (305, '723604', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'IT Costs')
        , (306, '723605', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Sales & Administration', '', '', '', '', '', 'IT Costs')
        , (307, '725100', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Freight')
        , (308, '725101', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Freight')
        , (309, '725102', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Freight')
        , (310, '725103', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Freight')
        , (311, '725104', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Freight')
        , (312, '725105', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Freight')
        , (313, '725106', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Freight')
        , (314, '725107', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Freight')
        , (315, '725200', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Storage')
        , (316, '725300', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Export Documentation')
        , (317, '725400', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Total Supply Chain', '', '', '', '', '', 'Cost to Serve')
        , (318, '726100', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Stock Adjustments', '', '', '', '', '', 'Stock Adjustments')
        , (319, '726101', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Stock Adjustments', '', '', '', '', '', 'Stock Adjustments')
        , (320, '726102', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Stock Adjustments', '', '', '', '', '', 'Stock Adjustments')
        , (321, '726103', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Stock Adjustments', '', '', '', '', '', 'Stock Adjustments')
        , (322, '726104', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Net Operating Profit', 'Stock Adjustments', '', '', '', '', '', 'Stock Adjustments')
        , (323, '727000', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Allocations')
        , (324, '727100', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Realised')
        , (325, '727101', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Realised')
        , (326, '727102', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Realised')
        , (327, '727103', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Realised')
        , (328, '727104', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Realised')
        , (329, '727105', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Realised')
        , (330, '727106', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Realised')
        , (331, '727200', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Unrealised')
        , (332, '727201', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Unrealised')
        , (333, '727202', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Unrealised')
        , (334, '727203', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Unrealised')
        , (335, '727204', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Unrealised')
        , (336, '727205', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Unrealised')
        , (337, '727206', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Foreign Exchange Gains and Losses', '', '', '', '', '', '', 'Foreign Exchange Gains and Losses Unrealised')
        , (338, '727400', -1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Normalised EBIT', 'Total Equity Accounted Earnings', '', '', '', '', '', '', 'Total Equity Accounted Earnings')
        , (339, '727600', -1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Asset Profit / Loss', 'Total Gains on Asset Disposals', '', '', '', '', '', '', 'Total Gains on Asset Disposals')
        , (340, '727601', -1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Asset Profit / Loss', 'Total Gains on Asset Disposals', '', '', '', '', '', '', 'Total Gains on Asset Disposals')
        , (341, '727700', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Asset Profit / Loss', 'Total Losses on Asset Disposals', '', '', '', '', '', '', 'Total Losses on Asset Disposals')
        , (342, '727701', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Asset Profit / Loss', 'Total Losses on Asset Disposals', '', '', '', '', '', '', 'Total Losses on Asset Disposals')
        , (343, '727800', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Impairment & Retstructuring', '', '', '', '', '', '', '', 'Impairment')
        , (344, '727801', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Impairment & Retstructuring', '', '', '', '', '', '', '', 'Impairment')
        , (345, '727802', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Impairment & Retstructuring', '', '', '', '', '', '', '', 'Impairment')
        , (346, '727803', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Impairment & Retstructuring', '', '', '', '', '', '', '', 'Impairment')
        , (347, '727804', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Impairment & Retstructuring', '', '', '', '', '', '', '', 'Impairment')
        , (348, '727900', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Impairment & Retstructuring', '', '', '', '', '', '', '', 'Restructuring')
        , (349, '727901', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Impairment & Retstructuring', '', '', '', '', '', '', '', 'Restructuring')
        , (350, '728000', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'EBIT', 'Total Abnormal Items', '', '', '', '', '', '', '', 'Total Abnormal Items')
        , (351, '729000', -1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Interest', '', '', '', '', '', '', '', 'Interest Received')
        , (352, '729001', -1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Interest', '', '', '', '', '', '', '', 'Interest Received')
        , (353, '729002', -1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Interest', '', '', '', '', '', '', '', 'Interest Received')
        , (354, '729100', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Interest', '', '', '', '', '', '', '', 'Interest Paid')
        , (355, '729101', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Interest', '', '', '', '', '', '', '', 'Interest Paid')
        , (356, '729102', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Interest', '', '', '', '', '', '', '', 'Interest Paid')
        , (357, '729103', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Interest', '', '', '', '', '', '', '', 'Interest Paid')
        , (358, '729104', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Interest', '', '', '', '', '', '', '', 'Interest Paid')
        , (359, '729105', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Interest', '', '', '', '', '', '', '', 'Interest Paid')
        , (360, '729106', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Interest', '', '', '', '', '', '', '', 'Interest Paid')
        , (361, '729300', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Dividends', '', '', '', '', '', '', '', 'Dividends Paid')
        , (362, '729301', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Dividends', '', '', '', '', '', '', '', 'Dividends Paid')
        , (363, '729302', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Dividends', '', '', '', '', '', '', '', 'Dividends Paid')
        , (364, '729303', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Dividends', '', '', '', '', '', '', '', 'Dividends Paid')
        , (365, '729200', -1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Dividends', '', '', '', '', '', '', '', 'Dividends Received')
        , (366, '729201', -1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Profit Before Tax', 'Total Interest and Dividends', 'Dividends', '', '', '', '', '', '', '', 'Dividends Received')
        , (367, '730000', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Total Corporate and Deferred Tax', '', '', '', '', '', '', '', '', '', 'Total Corporate and Deferred Tax')
        , (368, '730001', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Total Corporate and Deferred Tax', '', '', '', '', '', '', '', '', '', 'Total Corporate and Deferred Tax')
        , (369, '730002', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Total Corporate and Deferred Tax', '', '', '', '', '', '', '', '', '', 'Total Corporate and Deferred Tax')
        , (370, '730003', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Total Corporate and Deferred Tax', '', '', '', '', '', '', '', '', '', 'Total Corporate and Deferred Tax')
        , (371, '730004', 1, 'Profit and Loss', 'Total Profit and Loss', 'Profit After Tax', 'Total Corporate and Deferred Tax', '', '', '', '', '', '', '', '', '', 'Total Corporate and Deferred Tax')
        , (372, '730100', -1, 'Profit and Loss', 'Total Profit and Loss', 'Non Controlling Interest', '', '', '', '', '', '', '', '', '', '', 'Non Controlling Interest')
        , (373, '730101', -1, 'Profit and Loss', 'Total Profit and Loss', 'Non Controlling Interest', '', '', '', '', '', '', '', '', '', '', 'Non Controlling Interest')
        , (374, '730102', -1, 'Profit and Loss', 'Total Profit and Loss', 'Non Controlling Interest', '', '', '', '', '', '', '', '', '', '', 'Non Controlling Interest')
    ) as v (
        recid, mainaccountid, gl_calc_factor, gl_category_l0, gl_category_l1, gl_category_l2, gl_category_l3, gl_category_l4, gl_category_l5
        , gl_category_l6, gl_category_l7, gl_category_l8, gl_category_l9, gl_category_l10, gl_category_l11, gl_category_l12, gl_category_l13
    )

)

select
    {{ dbt_utils.generate_surrogate_key(['recid']) }} as dim_tm1_gl_category_level_sk
    , recid
    , convert(numeric(6), mainaccountid) as mainaccountid
    , gl_calc_factor
    , gl_category_l0
    , gl_category_l1
    , gl_category_l2
    , gl_category_l3
    , gl_category_l4
    , gl_category_l5
    , gl_category_l6
    , gl_category_l7
    , gl_category_l8
    , gl_category_l9
    , gl_category_l10
    , gl_category_l11
    , gl_category_l12
    , gl_category_l13
from ct
