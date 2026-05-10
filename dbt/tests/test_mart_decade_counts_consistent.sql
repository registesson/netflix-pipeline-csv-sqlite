-- Failing rows: movie_count + tv_show_count does not add up to title_count.
select *
from {{ ref('mart_titles_by_decade') }}
where title_count <> movie_count + tv_show_count
