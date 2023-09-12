{% snapshot inventory_snapshot %}

{{
  config(
    target_database = "DEV_DB",
    target_schema = "DBT_ZYADBENAMEURSANIMAXCOM",
    strategy='check',
    unique_key='product_id',
    check_cols=['inventory'],
   )
}}

  SELECT * FROM {{ source('postgres', 'products') }}

{% endsnapshot %}