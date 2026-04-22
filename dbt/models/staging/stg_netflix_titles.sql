with source_seed as (
    select *
    from {{ ref('netflix_titles') }}
)

select
    cast(show_id as varchar) as show_id,
    trim(title) as title,
    trim(type) as type,
    trim(country) as country,
    cast(release_year as integer) as release_year,
    try_cast(date_added as timestamp) as date_added,
    listed_in,
    description
from source_seed
where title is not null

