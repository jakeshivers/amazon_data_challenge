CREATE TABLE product (
    id serial primary key,
    product_id VARCHAR(255) UNIQUE NOT NULL,
    product_name VARCHAR(500) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ON
    UPDATE
        CURRENT_TIMESTAMP -- helps to track changes
);
CREATE TABLE product_scalar (
    id serial primary key,
    product_id VARCHAR(255),
    category VARCHAR(255) NOT NULL,
    rating numeric(
        3,
        2
    ) CHECK (
        rating BETWEEN 0AND 5
    ),
    rating_count rating_count INT CHECK (
        rating_count >= 0
    ),
    foreign key (product_id) references products(product_id),
    primary key (
        product_id,
        top_level_category
    )
);
