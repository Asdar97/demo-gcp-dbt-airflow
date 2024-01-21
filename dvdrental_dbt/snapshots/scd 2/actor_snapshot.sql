{% snapshot actor_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='actor_id',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

SELECT 
    * 
FROM {{ source('raw_dvdrental', 'actor') }}

{% endsnapshot %}