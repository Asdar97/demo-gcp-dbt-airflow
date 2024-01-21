{% snapshot store_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='store_id',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

SELECT 
    * 
FROM {{ source('raw_dvdrental', 'store') }}

{% endsnapshot %}