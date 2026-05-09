with stg as (
    select * from {{ ref('stg_netflix_titles') }}
)

select
    show_id,
    title,
    type,
    release_year,
    date_added,
    listed_in,
    description,
    -- first country when multiple are listed (e.g. "United States, India")
    trim(split_part(country, ',', 1))                                        as country_primary,
    coalesce(nullif(trim(split_part(country, ',', 1)), ''), 'Unknown')       as country_normalized,
    -- decade bucketing: 1994 → 1990, 2021 → 2020
    (release_year // 10) * 10                                                as decade,
    -- recency flag: titles released from 2015 onward
    release_year >= 2015                                                     as is_recent,
    -- number of genres in the listed_in field
    len(string_split(listed_in, ','))                                        as genre_count,
    -- calendar year the title was added to the platform (null when date_added is null)
    extract(year from date_added)::integer                                   as date_added_year
from stg