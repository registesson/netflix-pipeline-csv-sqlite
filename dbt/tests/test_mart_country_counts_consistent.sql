-- Failing rows: aggregated counts or year bounds are inconsistent.
select *
from {{ ref('mart_titles_by_country') }}
where title_count <> movie_count + tv_show_count
   or min_release_year > max_release_year

