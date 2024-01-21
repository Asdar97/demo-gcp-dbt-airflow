{% snapshot city_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='city_id',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

SELECT 
    * 
FROM {{ source('raw_dvdrental', 'city') }}

{% endsnapshot %}