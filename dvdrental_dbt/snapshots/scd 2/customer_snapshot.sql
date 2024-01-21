{% snapshot customer_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

SELECT 
    * 
FROM {{ source('raw_dvdrental', 'customer') }}

{% endsnapshot %}