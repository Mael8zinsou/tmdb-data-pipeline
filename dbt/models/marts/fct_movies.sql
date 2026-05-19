{#
    Table de faits : 1 ligne par film.
    Grain     : movie_id
    Mesures   : popularity, vote_average, vote_count, composite_score
    FKs       : release_date_id → dim_date, language_code → dim_language
    Dimension N-N (genre) gérée via bridge_movie_genre.
#}

with movies as (
    select * from {{ ref('int_movies_with_metrics') }}
)

select
    -- Clé primaire
    movie_id,

    -- Foreign keys
    case
        when release_date is not null
        then cast(replace(to_varchar(release_date, 'YYYY-MM-DD'), '-', '') as number)
        else null
    end as release_date_id,
    language_code,

    -- Attributs descriptifs
    title,
    original_title,
    release_date,
    release_year,
    release_decade,
    has_release_date,
    is_recent,
    popularity_tier,
    vote_tier,

    -- Mesures
    popularity,
    vote_average,
    vote_count,
    composite_score,

    -- Audit
    ingestion_date
from movies
