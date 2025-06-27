CREATE Database PersonalExpenseDB;

USE PersonalExpenseDB;

CREATE TABLE Users (
    UserID INT PRIMARY KEY,
    Name NVARCHAR(100),
    Email NVARCHAR(100)
);

CREATE TABLE Categories (
    CategoryID INT PRIMARY KEY,
    CategoryName NVARCHAR(50)
);

CREATE TABLE Expenses (
    ExpenseID INT PRIMARY KEY,
    UserID INT,
    CategoryID INT,
    Amount DECIMAL(10, 2),
    Date DATE,
    Notes NVARCHAR(MAX),
    FOREIGN KEY (UserID) REFERENCES Users(UserID),
    FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
);
