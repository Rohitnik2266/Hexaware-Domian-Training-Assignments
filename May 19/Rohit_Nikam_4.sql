use Products

CREATE TABLE ProductInventory (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Quantity INT,
    UnitPrice INT,
    Supplier VARCHAR(100),
    LastRestocked DATE
);

INSERT INTO ProductInventory VALUES
(1, 'Laptop', 'Electronics', 20, 70000, 'TechMart', '2025-04-20'),
(2, 'Office Chair', 'Furniture', 50, 5000, 'HomeComfort', '2025-04-18'),
(3, 'Smartwatch', 'Electronics', 30, 15000, 'GadgetHub', '2025-04-22'),
(4, 'Desk Lamp', 'Lighting', 80, 1200, 'BrightLife', '2025-04-25'),
(5, 'Wireless Mouse', 'Electronics', 100, 1500, 'GadgetHub', '2025-04-30');

--1. CRUD Operations:

INSERT INTO ProductInventory
VALUES (6, 'Gaming Keyboard', 'Electronics', 40, 3500, 'TechMart', '2025-05-01');

UPDATE ProductInventory
SET Quantity = Quantity + 20
WHERE ProductName = 'Desk Lamp';

DELETE FROM ProductInventory
WHERE ProductID = 2;

SELECT * FROM ProductInventory
ORDER BY ProductName ASC;

--2. Sorting and Filtering:

SELECT * FROM ProductInventory
ORDER BY Quantity DESC;

SELECT * FROM ProductInventory
WHERE Category = 'Electronics';

SELECT * FROM ProductInventory
WHERE Category = 'Electronics' AND Quantity > 20;

SELECT * FROM ProductInventory
WHERE Category = 'Electronics' OR UnitPrice < 2000;

--3. Aggregation and Grouping:

SELECT SUM(Quantity * UnitPrice) AS TotalStockValue
FROM ProductInventory;

SELECT Category, AVG(UnitPrice) AS AveragePrice
FROM ProductInventory
GROUP BY Category;

SELECT COUNT(*) AS ProductCount
FROM ProductInventory
WHERE Supplier = 'GadgetHub';

--4. Conditional and Pattern Matching:

SELECT * FROM ProductInventory
WHERE ProductName LIKE 'W%';

SELECT * FROM ProductInventory
WHERE Supplier = 'GadgetHub' AND UnitPrice > 10000;

SELECT * FROM ProductInventory
WHERE UnitPrice BETWEEN 1000 AND 20000;

--5. Advanced Queries:

SELECT TOP 3 * FROM ProductInventory
ORDER BY UnitPrice DESC;

SELECT * FROM ProductInventory
WHERE LastRestocked >= DATEADD(DAY, -10, GETDATE());

SELECT Supplier, SUM(Quantity) AS TotalQuantity
FROM ProductInventory
GROUP BY Supplier;

SELECT * FROM ProductInventory
WHERE Quantity < 30;

--6. Join and Subqueries (if related tables are present):

SELECT TOP 1 Supplier, COUNT(*) AS ProductCount
FROM ProductInventory
GROUP BY Supplier
ORDER BY ProductCount DESC;

SELECT TOP 1 *, (Quantity * UnitPrice) AS StockValue
FROM ProductInventory
ORDER BY StockValue DESC;

