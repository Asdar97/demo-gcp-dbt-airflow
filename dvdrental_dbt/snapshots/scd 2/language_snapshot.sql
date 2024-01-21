{% snapshot language_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='language_id',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

SELECT 
    * 
FROM {{ source('raw_dvdrental', 'language') }}

{% endsnapshot %}