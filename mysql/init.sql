CREATE DATABASE IF NOT EXISTS cdc_db;
USE cdc_db;

CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    price DECIMAL(10,2),
    stock INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO products (name,description,price,stock) VALUES
('Laptop','High performance laptop',1200,50),
('Mouse','Wireless mouse',25,200),
('Keyboard','Mechanical keyboard',75,100);
