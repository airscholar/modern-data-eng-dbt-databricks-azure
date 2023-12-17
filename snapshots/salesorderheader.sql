{% snapshot salesorderheader_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/salesorderheader",
      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='SalesOrderID',
      strategy='check',
      check_cols='all'
    )
}}

with salesorderheader_snapshot as (
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
        Comment
    FROM {{ source('saleslt', 'salesorderheader') }}
)

select * from salesorderheader_snapshot

{% endsnapshot %}