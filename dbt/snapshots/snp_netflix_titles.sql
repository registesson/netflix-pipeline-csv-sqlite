{% snapshot snp_netflix_titles %}

{{
    config(
        target_schema='snapshots',
        unique_key='show_id',
        strategy='check',
        check_cols=['title', 'type', 'country', 'release_year', 'listed_in'],
    )
}}

select *
from {{ ref('stg_netflix_titles') }}

{% endsnapshot %}