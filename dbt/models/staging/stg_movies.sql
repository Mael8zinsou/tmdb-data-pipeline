with src as (
    select * from {{ source('raw', 'movies_enriched') }}
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
    cast(ingestion_date as date) as ingestion_date
from src
