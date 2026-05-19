select
    country_code,
    country_name,
    country_native_name
from {{ source('raw', 'dim_country') }}
