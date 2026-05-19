select
    language_code,
    language_name,
    language_native_name
from {{ source('raw', 'dim_language') }}
