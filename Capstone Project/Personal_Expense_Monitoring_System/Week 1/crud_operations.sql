USE PersonalExpenseDB

-- Users
INSERT INTO Users VALUES (1, 'Rohit', 'rohit@example.com');
INSERT INTO Users VALUES (2, 'Priya', 'priya@example.com');

-- categories
INSERT INTO Categories VALUES 
(1, 'Grocery'), 
(2, 'Travel'), 
(3, 'Bills'), 
(4, 'Shopping'), 
(5, 'Food');

-- expenses
INSERT INTO Expenses VALUES 
(101, 1, 1, 1200.00, '2025-05-10', 'Big Bazaar purchase'),
(102, 1, 2, 450.00, '2025-05-12', 'Bus ticket'),
(103, 1, 3, 2000.00, '2025-05-15', 'Electricity bill'),
(104, 2, 4, 3000.00, '2025-05-20', 'Clothes at Myntra'),
(105, 2, 5, 600.00, '2025-05-25', 'Dinner at McDonald'),
(106, 1, 1, 800.00, '2025-06-02', 'Local grocery'),
(107, 1, 3, 1900.00, '2025-06-05', 'Water bill'),
(108, 2, 2, 1000.00, '2025-06-08', 'Uber ride'),
(109, 1, 5, 750.00, '2025-06-10', 'Zomato lunch'),
(110, 2, 4, 2200.00, '2025-06-12', 'Amazon sale');
