select
    genre_id,
    genre_name
from {{ source('raw', 'dim_genre') }}
