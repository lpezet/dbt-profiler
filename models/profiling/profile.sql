{{
    config(
        materialized=var("profile_materialization"),
        unique_key = "table_schema, table_name, field"
    )
}}
/*  */

{%- set query -%}
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

{%- set nums = run_query(query) -%}

{%- if execute -%}
{%- set numeric_items = nums.rows -%}
{%- endif -%}

{%- set query -%}
select
    sch,
    tbl,
    col
from
    {{ ref('profile_first_stage') }}
where
    1=1
    AND is_timestamp = 1
{%- endset -%}

{%- set nums = run_query(query) -%}

{%- if execute -%}
{%- set timestamp_items = nums.rows -%}
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
            is_timestamp,
            numeric_min,
            numeric_max,
            numeric_mean,
            numeric_std_dev,
            numeric_lower_quartile,
            numeric_median,
            numeric_upper_quartile,
            timestamp_min,
            timestamp_max,
            popular_patterns,
            rare_patterns
        from 
            source_data
        where
            is_numeric = 0
            AND is_timestamp = 0
        {%- for i in numeric_items %}
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
            is_timestamp,
            (SELECT CAST(MIN(`{{i[2]}}`) AS DECIMAL(65, 30)) FROM `{{ i[0] }}`.`{{ i[1] }}`) as numeric_min,
            (SELECT CAST(MAX(`{{i[2]}}`) AS DECIMAL(65, 30)) FROM `{{ i[0] }}`.`{{ i[1] }}`) as numeric_max,
            (SELECT CAST(ROUND(AVG(`{{i[2]}}`), {{var('precision')}}) AS DECIMAL(65, 30)) FROM `{{ i[0] }}`.`{{ i[1] }}`) as numeric_mean,
            (SELECT CAST(ROUND(STDDEV_POP(`{{i[2]}}`), {{var('precision')}}) AS DECIMAL(65, 30)) FROM `{{ i[0] }}`.`{{ i[1] }}`) as numeric_std_dev, 
            numeric_lower_quartile,
            numeric_median,
            numeric_upper_quartile,
            timestamp_min,
            timestamp_max,
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
        {%- for i in timestamp_items %}
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
            is_timestamp,
            numeric_min,
            numeric_max,
            numeric_mean,
            numeric_std_dev, 
            numeric_lower_quartile,
            numeric_median,
            numeric_upper_quartile,
            (SELECT CAST(MIN(`{{i[2]}}`) AS DATETIME) FROM `{{ i[0] }}`.`{{ i[1] }}`) as timestamp_min,
            (SELECT CAST(MAX(`{{i[2]}}`) AS DATETIME) FROM `{{ i[0] }}`.`{{ i[1] }}`) as timestamp_max,
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
    '{{version}}' as ver,
    r.*
from
    results r
order by table_schema, table_name, field
