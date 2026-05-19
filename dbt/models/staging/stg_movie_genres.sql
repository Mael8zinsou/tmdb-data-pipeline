select distinct
    movie_id,
    genre_id
from {{ source('raw', 'movie_genres') }}
where movie_id is not null
  and genre_id is not null
