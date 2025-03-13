CREATE TABLE products (
    id serial primary key,
    product_id VARCHAR(10) UNIQUE NOT NULL,
    product_name VARCHAR(500) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE product_scalars (
    id serial primary key,
    product_id VARCHAR(10),
    category VARCHAR(255) NOT NULL,
    rating numeric(
        3,
        2
    ) CHECK (
        rating BETWEEN 0
        AND 5
    ),
    rating_count INT CHECK (
        rating_count >= 0
    ),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    foreign key (product_id) references product(product_id)
);
