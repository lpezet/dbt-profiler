{{
    config(
        materialized=var("profile_materialization"),
        unique_key = "table_schema, table_name, field"
    )
}}
/*  */

{%- set num_query -%}
select
    sch,
    tbl,
    col
from
    {{ ref('profile_first_stage') }}
where
    1=1
    AND is_numeric = 1
{%- endset -%}

{%- set nums = run_query(num_query) -%}

{%- if execute -%}
{%- set items = nums.rows -%}
{%- endif -%}

with 
    source_data AS (
        select
            *
        from
            {{ ref('profile_first_stage') }}
        where
            col NOT LIKE '__dummy__%'
    ),
    results AS (
        select
            sch as table_schema,
            tbl as table_name,
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
            '{{ i[0] }}' as table_schema,
            '{{ i[1] }}' as table_name,
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
            (SELECT CAST(MIN(`{{i[2]}}`) AS DECIMAL(65, 30)) FROM `{{ i[0] }}`.`{{ i[1] }}`) as numeric_min,
            (SELECT CAST(MAX(`{{i[2]}}`) AS DECIMAL(65, 30)) FROM `{{ i[0] }}`.`{{ i[1] }}`) as numeric_max,
            (SELECT CAST(AVG(`{{i[2]}}`) AS DECIMAL(65, 30)) FROM `{{ i[0] }}`.`{{ i[1] }}`) as numeric_mean,
            (SELECT CAST(STDDEV_POP(`{{i[2]}}`) AS DECIMAL(65, 30)) FROM `{{ i[0] }}`.`{{ i[1] }}`) as numeric_std_dev, 
            numeric_lower_quartile,
            numeric_median,
            numeric_upper_quartile,
            popular_patterns,
            rare_patterns
        from
            source_data
        where
            1=1
            AND sch = '{{i[0]}}'
            AND tbl = '{{i[1]}}'
            AND col = '{{i[2]}}'
        {%- endfor %}
    )
select
    *
from
    results
order by table_schema, table_name, field
