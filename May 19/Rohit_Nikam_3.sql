use Products

create table EmployeeAttendance (
    AttendanceID INT PRIMARY KEY,
    EmployeeName VARCHAR(100),
    Department VARCHAR(100),
    Date DATE,
    Status VARCHAR(50),
    HoursWorked INT
);

insert into EmployeeAttendance values
(1, 'John Doe', 'IT', '2025-05-01', 'Present', 8),
(2, 'Priya Singh', 'HR', '2025-05-01', 'Absent', 0),
(3, 'Ali Khan', 'IT', '2025-05-01', 'Present', 7),
(4, 'Riya Patel', 'Sales', '2025-05-01', 'Late', 6),
(5, 'David Brown', 'Marketing', '2025-05-01', 'Present', 8);

--1. CRUD Operations:

insert into EmployeeAttendance values 
(6, 'Neha Sharma', 'Finance', '2025-05-01', 'Present', 8);

update EmployeeAttendance set Status = 'Present' 
where EmployeeName = 'Riya Patel' and Date = '2025-05-01';

delete from EmployeeAttendance
where EmployeeName = 'Priya Singh' and Date = '2025-05-01';

select * 
from EmployeeAttendance 
order by EmployeeName asc;

--2. Sorting and Filtering:

select * 
from EmployeeAttendance 
order by HoursWorked desc;

select * 
from EmployeeAttendance 
where Department = 'IT';

select * 
from EmployeeAttendance 
where Department = 'IT' and Status = 'Present';

select * 
from EmployeeAttendance 
where Status IN ('Absent', 'Late');

--3. Aggregation and Grouping:

select Department, SUM(HoursWorked) AS TotalHours 
from EmployeeAttendance 
group by Department;

select Date,AVG(HoursWorked) AS AvgHoursWorked
from EmployeeAttendance
group by Date 
order by Date;

select Status, COUNT(*) AS Count
from EmployeeAttendance 
group by Status;

--4. Conditional and Pattern Matching:

select * 
from EmployeeAttendance 
where EmployeeName like 'R%';

select * 
from EmployeeAttendance
where Status = 'Present' and HoursWorked > 6;

select *
from EmployeeAttendance
where HoursWorked between 6 and 8;

--5. Advanced Queries:

select top 2 * 
from EmployeeAttendance 
order by HoursWorked desc;

select * 
from EmployeeAttendance 
where HoursWorked < (
    select AVG(HoursWorked) 
	from EmployeeAttendance
);

select Status, AVG(HoursWorked) AS AvgHours
from EmployeeAttendance
group by Status;

select EmployeeName, Date, COUNT(*) AS EntryCount
from EmployeeAttendance
group by EmployeeName, Date 
having COUNT(*) > 1;

--6. Join and Subqueries (if related tables are present):

select top 1 Department, COUNT(*) AS PresentCount
from EmployeeAttendance 
where Status = 'Present' 
group by Department 
order by PresentCount desc;

select * 
from EmployeeAttendance E 
where HoursWorked = (
    select MAX(HoursWorked)
    from EmployeeAttendance
    where Department = E.Department
);
