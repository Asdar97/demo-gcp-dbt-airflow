{% snapshot film_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='film_id',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

SELECT 
    * 
FROM {{ source('raw_dvdrental', 'film') }}

{% endsnapshot %}