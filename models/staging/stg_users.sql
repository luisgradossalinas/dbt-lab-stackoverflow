with
    source as (
        select
            id as user_id,
            age,
            creation_date,
            website_url,
            round(
                timestamp_diff(current_timestamp(), creation_date, day) / 365
            ) as user_tenure
        from {{ source("stackoverflow", "users") }}
    )

select * from source
