{% snapshot customeraddress_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/customeraddress",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key="CustomerId||'-'||AddressId",
      strategy='check',
      check_cols='all'
    )
}}

with source_data as (
    select
        CustomerId,
        AddressId,
        AddressType
    from {{ source('saleslt', 'customeraddress') }}
)
select *
from source_data

{% endsnapshot %}