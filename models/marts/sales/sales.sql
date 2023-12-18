{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/gold/sales"
    )
}}

with salesorderdetail_snapshot as (
    SELECT
        SalesOrderID,
        SalesOrderDetailID,
        OrderQty,
        ProductID,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal
    FROM {{ ref("salesorderdetail_snapshot") }}
),

product_snapshot as (
    SELECT
        ProductID,
        Name,
        ProductNumber,
        Color,
        StandardCost,
        ListPrice,
        Size,
        Weight,
        SellStartDate,
        SellEndDate,
        DiscontinuedDate,
        ThumbNailPhoto,
        ThumbnailPhotoFileName
    FROM {{ source('saleslt', 'product') }}
),

saleorderheader_snapshot as (
    SELECT
        SalesOrderID,
        RevisionNumber,
        OrderDate,
        DueDate,
        ShipDate,
        Status,
        OnlineOrderFlag,
        SalesOrderNumber,
        PurchaseOrderNumber,
        AccountNumber,
        CustomerID,
        ShipToAddressID,
        BillToAddressID,
        ShipMethod,
        CreditCardApprovalCode,
        SubTotal,
        TaxAmt,
        Freight,
        TotalDue,
        Comment,
        row_number() over (partition by SalesOrderID order by SalesOrderID) as row_num
    FROM {{ source('saleslt', 'salesorderheader') }}
),

transformed as (
    select
        sod.SalesOrderID,
        sod.SalesOrderDetailID,
        sod.OrderQty,
        sod.ProductID,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        sod.LineTotal,
        p.Name,
        p.ProductNumber,
        p.Color,
        p.StandardCost,
        p.ListPrice,
        p.Size,
        p.Weight,
        p.SellStartDate,
        p.SellEndDate,
        p.DiscontinuedDate,
        p.ThumbNailPhoto,
        p.ThumbnailPhotoFileName,
        soh.RevisionNumber,
        soh.OrderDate,
        soh.DueDate,
        soh.ShipDate,
        soh.Status,
        soh.OnlineOrderFlag,
        soh.SalesOrderNumber,
        soh.PurchaseOrderNumber,
        soh.AccountNumber,
        soh.CustomerID,
        soh.ShipToAddressID,
        soh.BillToAddressID,
        soh.ShipMethod,
        soh.CreditCardApprovalCode,
        soh.SubTotal,
        soh.TaxAmt,
        soh.Freight,
        soh.TotalDue,
        soh.Comment
    from salesorderdetail_snapshot sod
    left join product_snapshot p on sod.ProductID = p.ProductID
    left join saleorderheader_snapshot soh on sod.SalesOrderID = soh.SalesOrderID
)

select * from transformed