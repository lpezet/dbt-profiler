-- This first stage was created because MySQL would evaluate all expressions within an IF() function.
-- So IF(@is_numeric = 1, AVG(field1), NULL) doesn't really work as it will run the AVG() even if @is_numeric = 0 it seems.

{%- set col_query -%}
select
    column_name
from
    {{ source('information_schema', 'columns') }}
where
    1=1
    AND table_name = '{{ var("table_name") }}'
    AND table_schema = '{{ var("table_schema") }}'
    {%- if var("table_columns") != '' -%}
    AND column_name IN ({{ var("table_columns") }})
    {%- endif -%}
{%- endset -%}

{%- set cols = run_query(col_query) -%}

{%- if execute -%}
{%- set items = cols.columns[0].values() -%}
{%- endif -%}

-- CTEs first
WITH
    stats_f_r_dummy AS (
        SELECT 1 as useless
    )
{%- for i in items %}
    , stats_f_r_{{i}}_modes AS (
        select
            {{i}} as __n,
            COUNT(*) as __f -- frequency
        from `{{ var("table_schema") }}`.`{{ var("table_name") }}`
        group by 1
        -- HAVING __f > 1
        order by 2 DESC
    )
    , stats_f_r_{{i}}_quartiles AS (
        select
            {{i}} as __n,
            ROW_NUMBER() OVER() as __rn
        from `{{ var("table_schema") }}`.`{{ var("table_name") }}`
        order by 1
    )
    , patterns_{{i}} AS (
        select
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE({{i}}, 
                    '[0-9]', '9' COLLATE utf8mb4_0900_ai_ci),
                '[A-Z]', 'A' COLLATE utf8mb4_0900_ai_ci), 
            '[a-z]', 'a' COLLATE utf8mb4_0900_ai_ci) as pattern,
            COUNT(*) as f
        from  `{{ var("table_schema") }}`.`{{ var("table_name") }}`
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
@is_numeric := (SUM(REGEXP_LIKE({{i}}, '[-+]?[0-9]+(\.[0-9]+)?')) = COUNT(*)) as _is_numeric,
@number_of_vals := SUM(IF({{i}} IS NULL OR CAST({{i}} AS CHAR) = '', 0, 1)) as _number_of_vals,
@number_of_rows := COUNT(*) as _number_of_rows,
@lower_quartile := ROUND(@number_of_rows * 0.25) as _lower_quartile,
@upper_quartile := ROUND(@number_of_rows * 0.75) as _upper_quartile,
@middle := ROUND(@number_of_rows * 0.5) as _middle,
'{{i}}' as col,
CAST(@number_of_rows AS UNSIGNED) as rec_count,
CAST(@number_of_vals as UNSIGNED) as fill_count,
ROUND(@number_of_vals / @number_of_rows, 2) as fill_rate,
COUNT(DISTINCT {{i}}) as cardinality,
/* cardinality_breakdown */
(select GROUP_CONCAT(__n SEPARATOR ', ') from (SELECT __n FROM stats_f_r_{{i}}_modes LIMIT 5) S) as modes,
(SELECT min(length(CAST({{i}} AS CHAR)))) as min_length, -- WARNING: problem with TEXT?
(SELECT max(length(CAST({{i}} AS CHAR)))) as max_length, -- WARNING: problem with TEXT?
(SELECT avg(length(CAST({{i}} AS CHAR)))) as ave_length, -- WARNING: problem with TEXT?
@is_numeric as is_numeric,
IF(@is_numeric = 1, MIN({{i}}), NULL) as numeric_min,
IF(@is_numeric = 1, MAX({{i}}), NULL) as numeric_max,

-- WARNING: Problem with MySQL when using AVG()/STDDEV_POP() on chars when running inside a CREATE TABLE statement (otherwise, as a standalone SELECT, it's fine)
-- Ideas to fix this: create other model(s) with the is_numeric flag on it.
-- Use it as a ref here and have 2 queries: one where is_numeric = 1 and those fields are computed like commented below, one for is_numeric = 0 and just pass NULL for those fields
CAST(NULL as DECIMAL(65, 30)) as numeric_mean,
CAST(NULL as DECIMAL(65, 30))  as nmeric_std_dev,
-- IF(@is_numeric = 1, AVG({{i}}), NULL) as numeric_mean,
-- IF(@is_numeric = 1, STDDEV_POP({{i}}), NULL) as numeric_std_dev,

IF(@is_numeric = 1, (select __n from stats_f_r_{{i}}_quartiles WHERE __rn = @lower_quartile), NULL) as numeric_lower_quartile,
IF(@is_numeric = 1, (select __n from stats_f_r_{{i}}_quartiles WHERE __rn = @middle), NULL) as numeric_median, -- not accurate implementation
IF(@is_numeric = 1, (select __n from stats_f_r_{{i}}_quartiles WHERE __rn = @upper_quartile), NULL) as numeric_upper_quartile,

(SELECT GROUP_CONCAT(pattern SEPARATOR ', ') FROM (SELECT * FROM patterns_{{i}} ORDER BY f DESC, pattern LIMIT 5) S) as popular_patterns,
(SELECT GROUP_CONCAT(pattern SEPARATOR ', ') FROM (SELECT * FROM patterns_{{i}} ORDER BY f ASC, pattern LIMIT 5) S) as rare_patterns


-- correlations
from
    `{{ var("table_schema") }}`.`{{ var("table_name") }}`
{%- endfor %}
/*
Source: https://github.com/hpcc-systems/DataPatterns#profile

Profile() is a function macro for profiling all or part of a dataset.
The output is a dataset containing the following information for each
profiled attribute:

     attribute               The name of the attribute
     given_attribute_type    The ECL type of the attribute as it was defined
                             in the input dataset
     best_attribute_type     An ECL data type that both allows all values
                             in the input dataset and consumes the least
                             amount of memory
     rec_count               The number of records analyzed in the dataset;
                             this may be fewer than the total number of
                             records, if the optional sampleSize argument
                             was provided with a value less than 100
     fill_count              The number of rec_count records containing
                             non-nil values; a 'nil value' is an empty
                             string, a numeric zero, or an empty SET; note
                             that BOOLEAN attributes are always counted as
                             filled, regardless of their value; also,
                             fixed-length DATA attributes (e.g. DATA10) are
                             also counted as filled, given their typical
                             function of holding data blobs
     fill_rate               The percentage of rec_count records containing
                             non-nil values; this is basically
                             fill_count / rec_count * 100
     cardinality             The number of unique, non-nil values within
                             the attribute
     cardinality_breakdown   For those attributes with a low number of
                             unique, non-nil values, show each value and the
                             number of records containing that value; the
                             lcbLimit parameter governs what "low number"
                             means
     modes                   The most common values in the attribute, after
                             coercing all values to STRING, along with the
                             number of records in which the values were
                             found; if no value is repeated more than once
                             then no mode will be shown; up to five (5)
                             modes will be shown; note that string values
                             longer than the maxPatternLen argument will
                             be truncated
     min_length              For SET datatypes, the fewest number of elements
                             found in the set; for other data types, the
                             shortest length of a value when expressed
                             as a string; null values are ignored
     max_length              For SET datatypes, the largest number of elements
                             found in the set; for other data types, the
                             longest length of a value when expressed
                             as a string; null values are ignored
     ave_length              For SET datatypes, the average number of elements
                             found in the set; for other data types, the
                             average length of a value when expressed
     popular_patterns        The most common patterns of values; see below
     rare_patterns           The least common patterns of values; see below
     is_numeric              Boolean indicating if the original attribute
                             was a numeric scalar or if the best_attribute_type
                             value was a numeric scaler; if TRUE then the
                             numeric_xxxx output fields will be
                             populated with actual values; if this value
                             is FALSE then all numeric_xxxx output values
                             should be ignored
     numeric_min             The smallest non-nil value found within the
                             attribute as a DECIMAL; this value is valid only
                             if is_numeric is TRUE; if is_numeric is FALSE
                             then zero will show here
     numeric_max             The largest non-nil value found within the
                             attribute as a DECIMAL; this value is valid only
                             if is_numeric is TRUE; if is_numeric is FALSE
                             then zero will show here
     numeric_mean            The mean (average) non-nil value found within
                             the attribute as a DECIMAL; this value is valid only
                             if is_numeric is TRUE; if is_numeric is FALSE
                             then zero will show here
     numeric_std_dev         The standard deviation of the non-nil values
                             in the attribute as a DECIMAL; this value is valid only
                             if is_numeric is TRUE; if is_numeric is FALSE
                             then zero will show here
     numeric_lower_quartile  The value separating the first (bottom) and
                             second quarters of non-nil values within
                             the attribute as a DECIMAL; this value is valid only
                             if is_numeric is TRUE; if is_numeric is FALSE
                             then zero will show here
     numeric_median          The median non-nil value within the attribute
                             as a DECIMAL; this value is valid only
                             if is_numeric is TRUE; if is_numeric is FALSE
                             then zero will show here
     numeric_upper_quartile  The value separating the third and fourth
                             (top) quarters of non-nil values within
                             the attribute as a DECIMAL; this value is valid only
                             if is_numeric is TRUE; if is_numeric is FALSE
                             then zero will show here
     correlations            A child dataset containing correlation values
                             comparing the current numeric attribute with all
                             other numeric attributes, listed in descending
                             correlation value order; the attribute must be
                             a numeric ECL datatype; non-numeric attributes
                             will return an empty child dataset; note that
                             this can be a time-consuming operation,
                             depending on the number of numeric attributes
                             in your dataset and the number of rows (if you
                             have N numeric attributes, then
                             N * (N - 1) / 2 calculations are performed,
                             each scanning all data rows)
*/
