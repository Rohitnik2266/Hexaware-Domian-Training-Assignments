-- 1. Create Database
CREATE DATABASE ExpenseDB;
GO

USE ExpenseDB;
GO

-- 2. Create Tables

-- Users Table
CREATE TABLE Users (
    UserID INT PRIMARY KEY IDENTITY(1,1),
    UserName NVARCHAR(100),
    Email NVARCHAR(100),
    CreatedAt DATETIME DEFAULT GETDATE()
);

-- Categories Table
CREATE TABLE Categories (
    CategoryID INT PRIMARY KEY IDENTITY(1,1),
    CategoryName NVARCHAR(100),
    Description NVARCHAR(255)
);

-- Expenses Table
CREATE TABLE Expenses (
    ExpenseID INT PRIMARY KEY IDENTITY(1,1),
    UserID INT FOREIGN KEY REFERENCES Users(UserID),
    CategoryID INT FOREIGN KEY REFERENCES Categories(CategoryID),
    Amount DECIMAL(10, 2),
    ExpenseDate DATE,
    Description NVARCHAR(255)
);

-- 3. Insert Users (Updated Names)
INSERT INTO Users (UserName, Email) VALUES
('Rohit Nikam', 'rohit@example.com'),
('Tharun Mohan', 'tharun@example.com'),
('Rakesh Thota', 'rakesh@example.com');

-- 4. Insert Categories
INSERT INTO Categories (CategoryName, Description) VALUES
('Groceries', 'Daily food and supplies'),
('Utilities', 'Electricity, Water, etc.'),
('Entertainment', 'Movies, subscriptions'),
('Transport', 'Fuel, bus, train fares');

-- 5. View Inserted IDs (optional, for reference)
SELECT * FROM Users;
SELECT * FROM Categories;

-- 6. Insert Sample Expenses for Rohit Nikam (UserID = 1)
INSERT INTO Expenses (UserID, CategoryID, Amount, ExpenseDate, Description) VALUES
(1, 1, 500.00, '2025-07-01', 'Groceries at SuperMart'),
(1, 2, 1200.00, '2025-07-24', 'Electricity bill'),
(1, 3, 300.00, '2025-07-26', 'Netflix subscription'),
(1, 1, 650.00, '2025-07-02', 'Monthly ration'),
(1, 4, 200.00, '2025-07-15', 'Fuel expense');

-- 7. View Expenses
SELECT * FROM Expenses;

-- 8. CRUD Operations Examples

-- UPDATE an expense (e.g., Update description for ExpenseID = 2)
UPDATE Expenses
SET Amount = 1250.00, Description = 'Updated Electricity bill'
WHERE ExpenseID = 2;

-- DELETE an expense (e.g., ExpenseID = 5)
DELETE FROM Expenses
WHERE ExpenseID = 5;

-- 9. Stored Procedure: Monthly Totals Per Category
CREATE PROCEDURE GetMonthlyTotalExpenses
    @UserID INT,
    @Month INT,
    @Year INT
AS
BEGIN
    SELECT 
        C.CategoryName,
        SUM(E.Amount) AS TotalAmount
    FROM Expenses E
    JOIN Categories C ON E.CategoryID = C.CategoryID
    WHERE 
        E.UserID = @UserID AND 
        MONTH(E.ExpenseDate) = @Month AND 
        YEAR(E.ExpenseDate) = @Year
    GROUP BY C.CategoryName;
END;
GO

-- 10. Execute the Stored Procedure (Rohit Nikam, June 2025)
EXEC GetMonthlyTotalExpenses @UserID = 1, @Month = 7, @Year = 2025;
