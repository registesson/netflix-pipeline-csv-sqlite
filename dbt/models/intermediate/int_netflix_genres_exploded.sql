with stg as (
    select * from {{ ref('stg_netflix_titles') }}
),

exploded as (
    select
        show_id,
        title,
        type,
        release_year,
        coalesce(nullif(trim(split_part(country, ',', 1)), ''), 'Unknown') as country_normalized,
        -- DuckDB unnests the array inline; trim removes leading/trailing spaces per genre
        trim(unnest(string_split(listed_in, ',')))                         as genre
    from stg
    where listed_in is not null
)

select * from exploded
where genre <> ''
