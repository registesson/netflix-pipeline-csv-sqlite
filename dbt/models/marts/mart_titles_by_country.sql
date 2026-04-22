with base as (
    select *
    from {{ ref('stg_netflix_titles') }}
)

select
    coalesce(nullif(country, ''), 'Unknown') as country,
    count(*) as title_count,
    sum(case when type = 'Movie' then 1 else 0 end) as movie_count,
    sum(case when type = 'TV Show' then 1 else 0 end) as tv_show_count,
    min(release_year) as min_release_year,
    max(release_year) as max_release_year
from base
group by 1
order by title_count desc

