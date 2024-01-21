{% snapshot category_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='category_id',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

SELECT 
    * 
FROM {{ source('raw_dvdrental', 'category') }}

{% endsnapshot %}