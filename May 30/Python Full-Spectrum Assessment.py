# Section 1: Python Basics & Control Flow
# Q1. Write a Python program to print all odd numbers between 10 and 50.

for i in range(10,51):
    if i%2!=0:
        print(i)

# Q2. Create a function that returns whether a given year is a leap year.

def leapyear(year):
    if year % 4 == 0 and year % 100 != 0 or year % 400 == 0:
        print("Leap Year")
    else:
        print("Not Leap Year")
a = int(input('Enter the year :'))
leapyear(a)

# Q3. Write a loop that counts how many times the letter a appears in a given string.

string = input("Enter a sentence :")
count = 0
for i in string:
    if i == 'a' or i == 'A':
        count += 1
print(count)

# Section 2: Collections (Lists, Tuples, Sets, Dicts)
# Q4. Create a dictionary from the following lists:
# keys = ['a', 'b', 'c']
# values = [100, 200, 300]

keys = ['a', 'b', 'c']
values = [100, 200, 300]
dict = dict(zip(keys, values))
print(dict)

# Q5. From a list of employee salaries, extract:
# The maximum salary
# All salaries above average
# A sorted version in descending order

salaries = [50000, 60000, 55000, 70000, 52000]
max = max(salaries)
print(max)
avg = sum(salaries)/len(salaries)
for i in salaries:
    if i > avg:
        print(i)
print(sorted(salaries,reverse=True))

# Q6. Create a set from a list and remove duplicates. Show the difference between two
# sets:
# a = [1, 2, 3, 4]
# b = [3, 4, 5, 6]

a = [1, 2, 3, 4]
b = [3, 4, 5, 6]
c = set(a)
d = set(b)
print(c)
print(d)
print('Difference :',c.difference(d))

# Section 3: Functions & Classes
# Q7. Write a class Employee with __init__ , display() , and is_high_earner() methods.
# An employee is a high earner if salary > 60000.

class Employee:
    def __init__(self, firstName, lastName, salary):
        self.firstName = firstName
        self.lastName = lastName
        self.salary = salary

    def display(self):
        print(f"First Name: {self.firstName}, Last Name: {self.lastName}, Salary: {self.salary}")

    def is_high_earner(self):
        if self.salary > 60000:
            print("Employee is high earner")
        else:
            print("Employee is Low earner")

e = Employee('Rohit', 'Nikam', 70000)
e.display()
e.is_high_earner()

# Q8. Create a class Project that inherits from Employee and adds project_name and hours_allocated .

class Project(Employee):
    def __init__(self,firstName, lastName, salary, projectName, hoursAllocated):
        super().__init__(firstName, lastName, salary)
        self.projectName = projectName
        self.hoursAllocated = hoursAllocated

    def display(self):
        print(f"Project Name: {self.projectName}, Hours Allocated: {self.hoursAllocated}")

p = Project('Rohit', 'Nikam', 70000, 'flutter', 24)
p.display()
p.is_high_earner()

# Q9. Instantiate 3 employees and print whether they are high earners.

e1 = Employee('Tharun', "Mohan",  50000)
e1.display()
e1.is_high_earner()
e2 = Employee('Rakesh', "Thota", 60000)
e2.display()
e2.is_high_earner()
e3 = Employee('Eswar', "Achari",  70000)
e3.display()
e3.is_high_earner()

# Section 4: File Handling
# Q10. Write to a file the names of employees who belong to the 'IT' department.

with open('sample.txt', 'w') as file:
    file.write('''[
    {"Name": "Ali", "Department": "HR"},
    {"Name": "Neha", "Department": "IT"},
    {"Name": "Ravi", "Department": "Finance"},
    {"Name": "Sara", "Department": "IT"},
    {"Name": "Vikram", "Department": "HR"}
]''')


# Q11. Read from a text file and count the number of words.

file = open('sample.txt', 'r' )
content = file.read()
print(content)
word_count = len(content.split())
print("Number of words:", word_count)

# Section 5: Exception Handling
# Q12. Write a program that accepts a number from the user and prints the square. Handle the case when input is not a number.

try:
    num = int(input("Enter a number: "))
    print('Square :', num*num)
except ValueError:
    print('Invalid Input. Please enter a number.')

# Q13. Handle a potential ZeroDivisionError in a division function.

try:
    num1 = int(input("Enter 1st number: "))
    num2 = int(input("Enter 2nd number: "))
    print('Division :', num1/num2)
except ZeroDivisionError:
    print('"Cannot divide by zero"')

# Section 6: Pandas – Reading & Exploring CSVs
# Q14. Load both employees.csv and projects.csv using Pandas.

import pandas as pd

employee = pd.read_csv('employees.csv')
project = pd.read_csv('projects.csv')
print(employee.head())
print(project.head())

# Q15. Display:
# First 2 rows of employees
# Unique values in the Department column
# Average salary by department

print(employee.head(2))
group = employee.groupby(employee['Department']).size().reset_index()
print(group)

# Q16. Add a column TenureInYears = current year - joining year.

employee['Year'] = pd.to_datetime(employee['JoiningDate']).dt.year
employee['TenureInYear'] = 2025 - employee['Year']
print(employee.head())

# Section 7: Data Filtering, Aggregation, and Sorting
# Q17. From employees.csv , filter all IT department employees with salary > 60000.

filter = employee[(employee['Department'] == 'IT') & (employee['Salary'] > 60000)]
print(filter)

# Q18. Group by Department and get:
# Count of employees
# Total Salary
# Average Salary

count = employee['Name'].count()
print(count)
total = employee['Salary'].sum()
print(total)
avg = employee['Salary'].mean()
print(avg)

# Q19. Sort all employees by salary in descending order.

sort = employee['Salary'].sort_values(ascending=False)
print(sort)

# Section 8: Joins & Merging
# Q20. Merge employees.csv and projects.csv on EmployeeID to show project allocations.

merge = pd.merge(employee, project, on='EmployeeID', how = 'inner')
print(merge)

# Q21. List all employees who are not working on any project (left join logic).

left = pd.merge(employee, project, on='EmployeeID', how = 'left')
print(left)

# Q22. Add a derived column TotalCost = HoursAllocated * (Salary / 160) in the merged dataset.

merge = pd.merge(employee, project, on='EmployeeID')
print(merge)
merge['TotalCost'] = merge['HoursAllocated'] * (merge['Salary']/160)
print(merge)