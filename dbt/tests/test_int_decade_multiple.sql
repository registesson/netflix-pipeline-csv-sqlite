-- Failing rows: decade is not a multiple of 10 (indicates a bucketing bug).
select *
from {{ ref('int_netflix_titles_enriched') }}
where decade % 10 <> 0