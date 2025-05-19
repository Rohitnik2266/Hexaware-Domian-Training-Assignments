use Products

create table Book(
BookID int primary key not null,
Title varchar(50),
Author varchar(50),
Genre varchar(50),
Price int,
PublishedYear int,
Stock int);

insert into Book values
(1, 'The Alchemist', 'Paulo Coelho', 'Fiction', 300, 1988, 50),
(2, 'Sapiens', 'Yuval Noah Harari', 'Non-Fiction', 500, 2011, 30),
(3, 'Atomic Habits', 'James Clear', 'Self-Help', 400, 2018, 25),
(4, 'Rich Dad Poor Dad', 'Robert Kiyosaki', 'Personal Finance', 350, 1997, 20),
(5, 'The Lean Startup', 'Eric Ries', 'Business', 450, 2011, 15);

--TASKS
--1. CRUD Operations:

insert into Book values
(6, 'Deep Work', 'Cal Newport', 'Self-Help', 420, 2016, 35);

update Book
set Price = Price+50
where Genre = 'Self-Help';

delete Book
where BookID = 4;

select *
from Book
order by Title asc;

--2. Sorting and Filtering:

select *
from Book
order by Price desc;

select *
from Book
where Genre = 'Fiction';

select *
from Book
where Genre = 'Self-Help' and Price > 400;

select *
from Book
where Genre = 'Fiction' or PublishedYear > 2000;

--3. Aggregation and Grouping:

select sum(Price * Stock) as total_value_of_all_books_in_stock
from Book;

select genre, avg(Price) as average_price
from Book
group by genre;

select count(*) as number_of_books
from Book
where Author = 'Paulo Coelho';

--4. Conditional and Pattern Matching:

select *
from Book
where Title like '%The%';

select *
from Book
where Author = 'Yuval Noah Harari' and Price < 600;

select *
from Book
where Price between 300 and 600;

--5. Advanced Queries:

select top 3 *
from Book
order by Price desc

select *
from Book
where PublishedYear < 2000;

select count(*) as total_number_of_books, Genre
from Book
group by Genre

select count(*)
from Book
group by Title
having count(*)>1;

--6. Join and Subqueries (if related tables are present)

select top 1 Author, count(*) as Book_Count
from Book
group by Author
order by Book_Count desc

select top 1 Genre
from Book
group by Genre
order by max(PublishedYear) desc