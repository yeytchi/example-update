{{config(
    schema= 'DATAQUALITY',
    materialized='incremental',
    unique_key='EXAMPLE_PK') }}

SELECT * FROM
(  SELECT
  {{ dbt_utils.surrogate_key(
    'ID',
    'PRODUCT_ID'
  ) }} as EXAMPLE_PK,
  ID,
  TIMESTAMP,
  PRODUCT_ID,
  CONVERT_TIMEZONE('UTC',current_timestamp())::timestamp as DT_DATAQUALITY,
  row_number() over (partition by ID, ACCOUNT_ID order by TIMESTAMP desc) as ROW_NUM
  FROM {{ ref('ST_EXAMPLE') }})
WHERE
  ROW_NUM = 1
    {% if is_incremental() %}
      -- this filter will only be applied on an incremental run
      and  DT_DATAQUALITY > (select max(DT_DATAQUALITY) from {{ this }})
    {% endif %}

-- DATAQUALITY RESULTS :
EXAMPLE_PK:'recordone', ID:2, timestamp:"2019-11-05T18:16:03+00:00", PRODUCT_ID:1, ROW_NUM:1
EXAMPLE_PK:'recordtwo', ID:2, timestamp:"2019-11-05T18:16:03+00:00", PRODUCT_ID:3, ROW_NUM:1
