-- Failing rows: release_year outside an expected modern range.
select *
from {{ ref('stg_netflix_titles') }}
where release_year < 1900
   or release_year > extract(year from current_date) + 1

