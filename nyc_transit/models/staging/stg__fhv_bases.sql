-- Create a list of staging commands to translate SQL commandes that modifies data type and column name 

with source as (
    select * from {{ source('main','fhv_bases') }}

),
renamed as (
    select 
        base_number,
        base_name,
        dba,
        dba_category,
        filename
    from source
)

select * from renamed
