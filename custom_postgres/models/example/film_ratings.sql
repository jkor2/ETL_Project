WITH films_with_ratings AS (
    SELECT
        film_id,
        title,
        release_date,
        price,
        rating,
        user_rating,
        CASE
            WHEN user_rating >= 4.5 THEN 'Excellent'
            WHEN user_rating >= 4.0 THEN 'Very Good'
            WHEN user_rating >= 3.0 THEN 'Good'
            ELSE 'Poor'
        END AS rating_category

    FROM {{ref('films')}}
)