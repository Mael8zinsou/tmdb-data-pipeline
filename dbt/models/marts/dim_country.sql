select
    country_code,
    country_name,
    country_native_name
from {{ ref('stg_countries') }}
