{#
    Couche intermediate : enrichit stg_movies avec :
      - score composite (popularity + vote_average pondéré par vote_count)
      - flag films "récents" (dernières 5 années glissantes)
    Sert de base à fct_movies.
#}

with movies as (
    select * from {{ ref('stg_movies') }}
)

select
    movie_id,
    title,
    original_title,
    overview,
    language_code,
    language_name,
    release_date,
    release_year,
    release_decade,
    has_release_date,
    popularity,
    popularity_tier,
    vote_average,
    vote_count,
    vote_tier,
    -- Score composite : popularité × log(vote_count) × note normalisée
    case
        when vote_count > 0
        then round(popularity * ln(vote_count + 1) * (vote_average / 10.0), 2)
        else 0.0
    end as composite_score,
    -- Catégorisation temporelle
    case
        when release_year >= year(current_date()) - 5 then true
        else false
    end as is_recent,
    ingestion_date
from movies
