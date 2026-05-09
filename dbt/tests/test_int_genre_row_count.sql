-- Failing rows: the exploded genre table has fewer rows than the staging table,
-- which would mean at least one title lost all its genres during the explode.
select 1 as sentinel
where (select count(*) from {{ ref('int_netflix_genres_exploded') }})
    < (select count(*) from {{ ref('stg_netflix_titles') }} where listed_in is not null)