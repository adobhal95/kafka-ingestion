DROP TABLE IF EXISTS PRODUCTS;

CREATE TABLE IF NOT EXISTS PRODUCTS(
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price NUMERIC(10, 2) NOT NULL,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO products (product_id, name, category, price)
VALUES
('P0001', 'Wireless Mouse', 'Electronics', 25.99),
('P0002', 'Bluetooth Headphones', 'Electronics', 59.49),
('P0003', 'Gaming Keyboard', 'Electronics', 89.99),
('P0004', 'LED Desk Lamp', 'Home & Living', 19.95),
('P0005', 'Cotton T-Shirt', 'Clothing', 12.50),
('P0006', 'Running Shoes', 'Sportswear', 75.00),
('P0007', 'Stainless Steel Water Bottle', 'Home & Living', 15.00),
('P0008', 'JavaScript: The Good Parts', 'Books', 29.99),
('P0009', 'Ergonomic Office Chair', 'Home & Living', 129.99),
('P0010', 'Yoga Mat', 'Sportswear', 22.00),
('P0011', 'Noise Cancelling Earbuds', 'Electronics', 45.99),
('P0012', 'Smartphone Tripod', 'Electronics', 18.75),
('P0013', 'Leather Wallet', 'Accessories', 34.95),
('P0014', 'Canvas Backpack', 'Accessories', 42.00),
('P0015', 'Graphic Hoodie', 'Clothing', 38.00),
('P0016', 'Electric Toothbrush', 'Personal Care', 49.99),
('P0017', 'Hair Dryer', 'Personal Care', 27.50),
('P0018', 'Cooking Essentials Set', 'Home & Living', 58.00),
('P0019', 'Puzzle Game Set', 'Toys & Games', 16.25),
('P0020', 'Portable Charger', 'Electronics', 21.99),
('P0021', 'Insulated Lunch Bag', 'Accessories', 17.00),
('P0022', 'Soft Plush Blanket', 'Home & Living', 32.50),
('P0023', 'Men''s Analog Watch', 'Accessories', 85.00),
('P0024', 'E-reader Kindle', 'Electronics', 119.00),
('P0025', 'Desk Organizer Set', 'Home & Living', 14.75),
('P0026', 'Hiking Backpack', 'Sportswear', 89.99),
('P0027', 'Acrylic Paint Set', 'Art Supplies', 26.40),
('P0028', 'Wooden Cutting Board', 'Kitchen', 19.99),
('P0029', 'LED Monitor 24-inch', 'Electronics', 139.50),
('P0030', 'USB-C Hub', 'Electronics', 34.99);