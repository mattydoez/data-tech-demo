-- Create Google Search table
CREATE TABLE google_search (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    campaign VARCHAR(100),
    ad VARCHAR(100),
    impressions INT,
    clicks INT,
    cost DECIMAL(10, 2),
    conversions INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Email Marketing table
CREATE TABLE email_marketing (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    user_email VARCHAR(100),
    campaign VARCHAR(100),
    ad VARCHAR(100),
    received INT,
    opened INT,
    subscribed INT,
    clicks INT,
    bounces INT,
    unsubscribed INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Facebook table
CREATE TABLE facebook (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50), -- 'Instagram' or 'Facebook'
    date DATE NOT NULL,
    campaign VARCHAR(100),
    ad_unit VARCHAR(100),
    impressions INT,
    percent_watched DECIMAL(5, 2),
    clicks INT,
    cost DECIMAL(10, 2),
    conversions INT,
    likes INT,
    shares INT,
    comments INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Users table
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100),
    address VARCHAR(255),
    phone_number VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Transactions table
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    product_id INT,
    user_id INT REFERENCES users(user_id),
    payment_method VARCHAR(50),
    transaction_type INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Clickstream Events table
CREATE TABLE clickstream_events (
    event_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    timestamp TIMESTAMP NOT NULL,
    page_visited VARCHAR(255),
    action VARCHAR(50),
    product_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category INT,
    description VARCHAR(255),
    price DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);