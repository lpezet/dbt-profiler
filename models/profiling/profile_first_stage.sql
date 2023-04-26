-- This first stage was created because MySQL would evaluate all expressions within an IF() function.
-- So IF(@is_numeric = 1, AVG(field1), NULL) doesn't really work as it will run the AVG() even if @is_numeric = 0 it seems.

{%- set col_query -%}
select
    table_name,
    column_name
from
    {{ source('information_schema', 'columns') }}
where
    1=1
    AND table_schema = '{{ var("table_schema") }}'
    {%- if var("table_name") != '' -%}
    AND table_name = '{{ var("table_name") }}'
    {%- endif -%}
    {%- if var("table_columns") != '' -%}
    AND column_name IN ({{ var("table_columns") }})
    {%- endif -%}
{%- endset -%}

{%- set results = run_query(col_query) -%}

{%- if execute -%}
{# set items = cols.columns[0].values() #}
{%- set items = results.rows -%}
{%- endif -%}

-- CTEs first
WITH
    stats_f_r_dummy AS (
        SELECT '{{ var("table_schema") }}' as table_schema, '{{ var("table_name") }}' as table_name,  '{{ var("table_columns") }}' as table_columns, {{items | length}} as items
    )
{%- for i in items %}
    , `stats_f_r_{{i[0]}}_{{i[1]}}_modes` AS (
        select
            CAST(
                CONCAT(
                    SUBSTRING(`{{i[1]}}`, 1, 100),
                    IF(LENGTH(`{{i[1]}}`) > 100, '...', '')
                    ) 
                AS CHAR) as __n,
            COUNT(*) as __f -- frequency
        from `{{ var("table_schema") }}`.`{{i[0]}}`
        group by 1
        -- HAVING __f > 1
        order by 2 DESC
    )
    , `stats_f_r_{{i[0]}}_{{i[1]}}_quartiles` AS (
        select
            CAST(
                CONCAT(
                    SUBSTRING(`{{i[1]}}`, 1, 100),
                    IF(LENGTH(`{{i[1]}}`) > 100, '...', '')
                    )  
                AS CHAR) as __n,
            ROW_NUMBER() OVER() as __rn
        from `{{ var("table_schema") }}`.`{{i[0]}}`
        order by 1
    )
    , `patterns_{{i[0]}}_{{i[1]}}` AS (
        select
            CONCAT(
                SUBSTRING(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                            `{{i[1]}}`,
                            '[0-9]', '9' COLLATE utf8mb4_0900_ai_ci),
                        '[A-Z]', 'A' COLLATE utf8mb4_0900_ai_ci), 
                    '[a-z]', 'a' COLLATE utf8mb4_0900_ai_ci),
                    1, 100
                ),
                IF(LENGTH(`{{i[1]}}`) > 100, '...', '')
            ) as pattern,
            COUNT(*) as f
        from  `{{ var("table_schema") }}`.`{{i[0]}}`
        group by 1
    )
{%- endfor %}
-- SELECT second
select
    '' as _is_numeric, -- user variables
    '' as _number_of_vals, -- user variables
    '' as _number_of_rows, -- user variables
    '' as _lower_quartile, -- user variables
    '' as _upper_quartile, -- user variables
    '' as _middle, -- user variables
    '__dummy__' as tbl,
    '__dummy__' as col,
    0 as rec_count,
    0 as fill_count,
    0 as fill_rate,
    0 as cardinality,
    '' as modes,
    0 as min_length,
    0 as max_length,
    0 as ave_length,
    0 as is_numeric,
    NULL as numeric_min,
    NULL as numeric_max,
    NULL as numberic_mean,
    NULL as numeric_std_dev,
    NULL as numeric_lower_quartile,
    NULL as numeric_median,
    NULL as numeric_upper_quartile,
    '' COLLATE utf8mb4_0900_ai_ci as popular_patterns,
    '' COLLATE utf8mb4_0900_ai_ci as rare_patterns
{%- for i in items %}
union all
select
@is_numeric := (SUM(REGEXP_LIKE(`{{i[1]}}`, '^[-+]?[0-9]+(\.[0-9]+)?$')) = COUNT(*)) as _is_numeric,
@number_of_vals := SUM(IF(`{{i[1]}}` IS NULL OR CAST(`{{i[1]}}` AS CHAR) = '', 0, 1)) as _number_of_vals,
@number_of_rows := COUNT(*) as _number_of_rows,
@lower_quartile := ROUND(@number_of_rows * 0.25) as _lower_quartile,
@upper_quartile := ROUND(@number_of_rows * 0.75) as _upper_quartile,
@middle := ROUND(@number_of_rows * 0.5) as _middle,
'{{i[0]}}' as tbl,
'{{i[1]}}' as col,
CAST(@number_of_rows AS UNSIGNED) as rec_count,
CAST(@number_of_vals as UNSIGNED) as fill_count,
ROUND(@number_of_vals / @number_of_rows, 2) as fill_rate,
COUNT(DISTINCT `{{i[1]}}`) as cardinality,
/* cardinality_breakdown */
(select GROUP_CONCAT(__n SEPARATOR ', ') from (SELECT __n FROM `stats_f_r_{{i[0]}}_{{i[1]}}_modes` LIMIT 5) S) as modes,
(SELECT min(length(CAST(`{{i[1]}}` AS CHAR)))) as min_length, -- WARNING: problem with TEXT?
(SELECT max(length(CAST(`{{i[1]}}` AS CHAR)))) as max_length, -- WARNING: problem with TEXT?
(SELECT avg(length(CAST(`{{i[1]}}` AS CHAR)))) as ave_length, -- WARNING: problem with TEXT?
@is_numeric as is_numeric,
-- IF(@is_numeric = 1, MIN(`{{i[1]}}`), NULL) as numeric_min,
-- IF(@is_numeric = 1, MAX(`{{i[1]}}`), NULL) as numeric_max,
CAST(NULL AS DECIMAL(65,30)) as numeric_min,
CAST(NULL AS DECIMAL(65,30)) as numeric_max,

-- WARNING: Problem with MySQL when using AVG()/STDDEV_POP() on chars when running inside a CREATE TABLE statement (otherwise, as a standalone SELECT, it's fine)
-- Ideas to fix this: create other model(s) with the is_numeric flag on it.
-- Use it as a ref here and have 2 queries: one where is_numeric = 1 and those fields are computed like commented below, one for is_numeric = 0 and just pass NULL for those fields
CAST(NULL as DECIMAL(65, 30)) as numeric_mean,
CAST(NULL as DECIMAL(65, 30))  as nmeric_std_dev,
-- IF(@is_numeric = 1, AVG({{i}}), NULL) as numeric_mean,
-- IF(@is_numeric = 1, STDDEV_POP({{i}}), NULL) as numeric_std_dev,

IF(@is_numeric = 1, (select __n from `stats_f_r_{{i[0]}}_{{i[1]}}_quartiles` WHERE __rn = @lower_quartile), NULL) as numeric_lower_quartile,
IF(@is_numeric = 1, (select __n from `stats_f_r_{{i[0]}}_{{i[1]}}_quartiles` WHERE __rn = @middle), NULL) as numeric_median, -- not accurate implementation
IF(@is_numeric = 1, (select __n from `stats_f_r_{{i[0]}}_{{i[1]}}_quartiles` WHERE __rn = @upper_quartile), NULL) as numeric_upper_quartile,

-- doing SUBSTRING() here as it can lead to "1260 (HY000): Row 2 was cut by GROUP_CONCAT()"
(SELECT GROUP_CONCAT(CAST(pattern AS CHAR) SEPARATOR ', ') FROM (SELECT * FROM `patterns_{{i[0]}}_{{i[1]}}` ORDER BY f DESC, pattern LIMIT 5) S) COLLATE utf8mb4_0900_ai_ci as popular_patterns,
(SELECT GROUP_CONCAT(CAST(pattern AS CHAR) SEPARATOR ', ') FROM (SELECT * FROM `patterns_{{i[0]}}_{{i[1]}}` ORDER BY f ASC, pattern LIMIT 5) S) COLLATE utf8mb4_0900_ai_ci as rare_patterns


-- correlations
from
    `{{ var("table_schema") }}`.`{{i[0]}}`
{%- endfor %}
