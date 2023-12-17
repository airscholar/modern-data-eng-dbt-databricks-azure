{% snapshot customer_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/customer",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='CustomerId',
      strategy='check',
      check_cols='all'
    )
}}

with source_data as (
    select
        CustomerId,
        NameStyle,
        Title,
        FirstName,
        MiddleName,
        LastName,
        Suffix,
        CompanyName,
        SalesPerson,
        EmailAddress,
        Phone,
        PasswordHash,
        PasswordSalt
    from {{ source('saleslt', 'customer') }}
)
select *
from source_data

{% endsnapshot %}