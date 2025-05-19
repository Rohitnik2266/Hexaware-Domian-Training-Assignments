create database Products;

use Products;

create table Product(
ProductID int primary key not null,
ProductName varchar(50),
Category varchar(50),
Price int,
StockQuantity int,
Supplier varchar(50));

insert into Product values
(1, 'Laptop', 'Electronics', 70000, 50, 'TechMart'),
(2, 'Office Chair', 'Furniture', 5000, 100, 'HomeComfort'),
(3, 'Smartwatch', 'Electronics', 15000, 200, 'GadgetHub'),
(4, 'Desk Lamp', 'Lighting', 1200, 300, 'BrightLife'),
(5, 'Wireless Mouse', 'Electronics', 1500, 250, 'GadgetHub');

--Tasks
--1. CRUD Operations:

insert into Product values(6, 'Gaming Keyboard ', 'Electronics', 3500, 150, 'TechMart');

update Product 
set Price = Price * 0.10
where Category = 'Electronics';

delete from Product
where ProductID = 4;

select *
from Product 
order by Price desc;

--2. Sorting and Filtering:

select *
from Product 
order by StockQuantity asc;

select *
from Product
where Category = 'Electronics';

select *
from Product
where Category = 'Electronics' and Price > 5000;

select *
from Product
where Category = 'Electronics' or Price < 2000;

--3. Aggregation and Grouping:

select sum(Price*StockQuantity) as total_stock_value 
from Product;

select Category, avg(Price) as average_price
from Product 
group by Category;

select count(*) as Total_product
from Product
where Supplier = 'GadgetHub';

--4. Conditional and Pattern Matching:

select ProductName 
from Product
where ProductName like '%Wireless%';

select *
from Product 
where Supplier in ('TechMart','GadgetHub');

select * 
from Product 
where Price between 1000 and 2000;

--5. Advanced Queries:

select *
from Product 
where StockQuantity > (
select avg(StockQuantity) as AvgStockQuantity
from Product);

select top 3 *
from Product 
order by Price;

select Supplier , Count(*) as Product_Count
from product 
group by Supplier
having count(*) > 1;

select Category , sum(Price *StockQuantity) as total_stock_value, count(ProductName) number_of_products
from Product
group by Category;

--6. Join and Subqueries (if related tables are present):

select top 1 Supplier, count(ProductName) Max_number_of_products
from Product
group by Supplier;

select P.*
from Product P
INNER JOIN (
    select Category, max(Price) as Max_Price
    from Product
    group by Category
) as Max_P
on P.Category = Max_P.Category AND P.Price = Max_P.Max_Price;
