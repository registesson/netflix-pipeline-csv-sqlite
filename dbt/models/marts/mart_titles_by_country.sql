with enriched as (
    select * from {{ ref('int_netflix_titles_enriched') }}
)

select
    country_normalized                                                    as country,
    count(*)                                                              as title_count,
    sum(case when type = 'Movie'   then 1 else 0 end)                    as movie_count,
    sum(case when type = 'TV Show' then 1 else 0 end)                    as tv_show_count,
    min(release_year)                                                     as min_release_year,
    max(release_year)                                                     as max_release_year,
    sum(case when is_recent then 1 else 0 end)                           as recent_titles_count,
    round(avg(genre_count), 1)                                           as avg_genre_count
from enriched
group by 1
order by title_count desc