with genres as (
    select * from {{ ref('int_netflix_genres_exploded') }}
)

select
    genre,
    count(*)                                                              as title_count,
    sum(case when type = 'Movie'   then 1 else 0 end)                    as movie_count,
    sum(case when type = 'TV Show' then 1 else 0 end)                    as tv_show_count,
    min(release_year)                                                     as min_release_year,
    max(release_year)                                                     as max_release_year
from genres
group by 1
order by title_count desc