{{config(
    schema= 'STAGING_DATAQUALITY',
    materialized='incremental',
    unique_key='EXAMPLE_PK') }}

with expanded as (
  select
    raw:id::integer as ID,
    raw:timestamp::timestamp as TIMESTAMP,
    value
    FROM {{ source('RAW', 'RAW_EXAMPLE') }}, lateral flatten( input => raw:items)
      where 1=1
      and raw:items is not null
)
SELECT
  {{ dbt_utils.surrogate_key(
    'ID',
    'value:product_id::integer'
  ) }} as EXAMPLE_PK,
  ID,
  TIMESTAMP,
  value:product_id::integer as PRODUCT_ID
  FROM expanded
  {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where TIMESTAMP > (select max(TIMESTAMP) from {{ this }})
  {% endif %}

-- MESSAGES :
{id:1,timestamp:"2019-11-04T18:14:03+00:00",items:[{product_id:1},{product_id:2},{product_id:3}]},
{id:2,timestamp:"2019-11-05T18:16:03+00:00",items:[{product_id:1},{product_id:3}]}

-- STAGING RESULTS :
EXAMPLE_PK:'firstrecord', ID:1, timestamp:"2019-11-04T18:14:03+00:00", PRODUCT_ID:1
EXAMPLE_PK:'secondrecord', ID:1, timestamp:"2019-11-04T18:14:03+00:00", PRODUCT_ID:2
EXAMPLE_PK:'thirdrecord', ID:1, timestamp:"2019-11-04T18:14:03+00:00", PRODUCT_ID:3
EXAMPLE_PK:'fourthrecord', ID:2, timestamp:"2019-11-05T18:16:03+00:00", PRODUCT_ID:1
EXAMPLE_PK:'fifthrecord', ID:2, timestamp:"2019-11-05T18:16:03+00:00", PRODUCT_ID:3
