select
    language_code,
    language_name,
    language_native_name
from {{ ref('stg_languages') }}
