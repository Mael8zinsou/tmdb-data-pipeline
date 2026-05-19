select
    genre_id,
    genre_name
from {{ ref('stg_genres') }}
