CREATE PROCEDURE GetMonthlyExpenseSummary
AS
BEGIN
    SELECT 
        FORMAT(Date, 'yyyy-MM') AS Month,
        c.CategoryName,
        SUM(e.Amount) AS Total
    FROM Expenses e
    JOIN Categories c ON e.CategoryID = c.CategoryID
    GROUP BY FORMAT(Date, 'yyyy-MM'), c.CategoryName
    ORDER BY Month, CategoryName;
END;
