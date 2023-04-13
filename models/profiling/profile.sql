{{
    config(
        materialized=var("profile_materialization"),
        unique_key = "table_schema, table_name, field"
    )
}}
/*  */

{%- set num_query -%}
select
    col
from
    {{ ref('profile_first_stage') }}
where
    1=1
    AND is_numeric = 1
{%- endset -%}

{%- set nums = run_query(num_query) -%}

{%- if execute -%}
{%- set items = nums.columns[0].values() -%}
{%- endif -%}

with 
    source_data AS (
        select
            *
        from
            {{ ref('profile_first_stage') }}
        where
            col != '__dummy__'
    )
select
    '{{ var("table_schema") }}' as table_schema,
    '{{ var("table_name") }}' as table_name,
    col as field,
    rec_count,
    fill_count,
    fill_rate,
    cardinality,
    modes,
    min_length,
    max_length,
    ave_length,
    is_numeric,
    numeric_min,
    numeric_max,
    numberic_mean,
    numeric_std_dev,
    numeric_lower_quartile,
    numeric_median,
    numeric_upper_quartile,
    popular_patterns,
    rare_patterns
from 
    source_data
where
    is_numeric = 0
{%- for i in items %}
union all
select
    '{{ var("table_schema") }}' as table_schema,
    '{{ var("table_name") }}' as table_name,
    col as field,
    rec_count,
    fill_count,
    fill_rate,
    cardinality,
    modes,
    min_length,
    max_length,
    ave_length,
    is_numeric,
    numeric_min,
    numeric_max,
    (SELECT CAST(AVG({{i}}) AS DECIMAL(65, 30)) FROM `{{ var("table_schema") }}`.`{{ var("table_name") }}`) as numeric_mean,
    (SELECT CAST(STDDEV_POP({{i}}) AS DECIMAL(65, 30)) FROM `{{ var("table_schema") }}`.`{{ var("table_name") }}`) as numeric_std_dev, 
    numeric_lower_quartile,
    numeric_median,
    numeric_upper_quartile,
    popular_patterns,
    rare_patterns
from
    source_data
   
where
    col = '{{i}}'
{%- endfor %}


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
