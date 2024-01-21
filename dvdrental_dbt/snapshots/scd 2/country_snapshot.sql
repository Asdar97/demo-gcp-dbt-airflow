{% snapshot country_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='country_id',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

SELECT 
    * 
FROM {{ source('raw_dvdrental', 'country') }}

{% endsnapshot %}