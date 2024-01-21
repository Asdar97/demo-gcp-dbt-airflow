{% snapshot staff_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='staff_id',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

SELECT 
    * 
FROM {{ source('raw_dvdrental', 'staff') }}

{% endsnapshot %}