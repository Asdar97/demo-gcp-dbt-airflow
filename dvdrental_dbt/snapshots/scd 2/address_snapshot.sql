{% snapshot address_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='address_id',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

SELECT 
    * 
FROM {{ source('raw_dvdrental', 'address') }}

{% endsnapshot %}