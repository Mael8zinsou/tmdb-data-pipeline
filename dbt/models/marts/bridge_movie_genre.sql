{#
    Table de pont : relation N-N entre films et genres.
    Permet d'éviter de stocker plusieurs FK genre dans fct_movies.
#}

select
    movie_id,
    genre_id
from {{ ref('stg_movie_genres') }}
