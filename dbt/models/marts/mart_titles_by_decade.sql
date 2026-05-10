with enriched as (
    select * from {{ ref('int_netflix_titles_enriched') }}
)

select
    decade,
    count(*)                                                    as title_count,
    sum(case when type = 'Movie'   then 1 else 0 end)           as movie_count,
    sum(case when type = 'TV Show' then 1 else 0 end)           as tv_show_count,
    count(distinct country_normalized)                          as country_count,
    round(avg(genre_count), 1)                                  as avg_genre_count
from enriched
group by 1
order by decade desc
