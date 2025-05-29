# FUNCTIONS (Exercises 1–3)
# 1. Prime Number Checker
# Write a function is_prime(n) that returns True if n is prime, else False . Use it to print all prime numbers between 1 and 100.
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2,int(n**0.5)+1):
        if n % i == 0:
            return False
    return True

print('Prime Number in 1 to 100 are :')
for i in range(1,101):
    if is_prime(i):
        print(i, end=" ")

# 2. Temperature Converter
# Write a function convert_temp(value, unit) that converts: Celsius to Fahrenheit, Fahrenheit to Celsius Use conditionals inside the function.
def convert_temp(value, unit):
    if unit == "C":
        return (value * 9 / 5) + 32
    elif unit == "F":
        return (value - 32) * 5 / 9
    else:
        return "Invalid unit"

unit = input('Enter the unit :')
value = int(input('Enter the value : '))
result = convert_temp(value, unit)
print('Converted temp :',result)

# 3. Recursive Factorial Function
# Create a function factorial(n) using recursion to return the factorial of a number.
def factorial(n):
    if n == 0 or n == 1:
        return 1
    return n * factorial(n - 1)

n = int(input('Enter the number : '))
result = factorial(n)

# CLASSES (Exercises 4–7)
# 4. Class: Rectangle
# Attributes: length , width
# Methods:
# area()
# perimeter()
# is_square() → returns True if length == width

class Rectangle:
    def __init__(self, width, length):
        self.width = width
        self.length = length

    def area(self):
        return self.width * self.length

    def perimeter(self):
        return 2 * (self.width + self.length)

    def is_square(self):
        return self.width == self.length

l = int(input('Enter the length : '))
w = int(input('Enter the width : '))
r = Rectangle(w, l)
print('Area of rectangle : ', r.area())
print('Perimeter of rectangle : ', r.perimeter())
print('Is rectangle square : ', r.is_square())

# 5. Class: BankAccount
# Attributes: name , balance
# Methods:
# deposit(amount)
# withdraw(amount)
# get_balance()
# Prevent withdrawal if balance is insufficient

class BankAccount:
    def __init__(self, name, balance):
        self.name = name
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount
        return self.balance

    def withdraw(self, amount):
        if self.balance < amount:
            print('Insufficient balance')
        else:
            self.balance -= amount
        return self.balance

    def get_balance(self):
        return self.balance

name = input('Enter your name: ')
b = BankAccount(name, 10000)
while True:
    print('A. Deposit\n')
    print('B. Withdraw\n')
    print('C. Balance\n')
    choice = input('Enter the option (A,B,C) :')

    if choice == 'A':
        amount = int(input('Deposit amount'))
        b.deposit(amount)
    elif choice == 'B':
        amount = int(input('Withdraw amount'))
        b.withdraw(amount)
    elif choice == 'C':
        print('Balance :', b.get_balance())
        break
    else:
        print('Invalid option')

# 6. Class: Book
# Attributes: title , author , price , in_stock
# Method: sell(quantity)
# Reduces stock
# Throws an error if quantity exceeds stock

class Book:
    def __init__(self, title, author, price, in_stock):
        self.title = title
        self.author = author
        self.price = price
        self.in_stock = in_stock

    def sell(self, quantity):
        if quantity > self.in_stock:
            raise ValueError('Not enough Stock')
        self.in_stock -= quantity

t = input('Enter the title of the book : ')
a = input('Enter the author of the book : ')
b = Book(t, a, 100, 10)
s = int(input('How mant needed : '))
b.sell(s)
print('Book left in stock',b.in_stock)

# 7. Student Grade System
#
# Create a class Student with:
# Attributes: name , marks (a list)
# Method:
# average()
# grade() — returns A/B/C/F based on average

class Students:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks

    def average(self):
        return sum(self.marks) / len(self.marks)

    def grade(self):
        average = self.average()
        if average >= 90:
            return 'A'
        elif average >= 60:
            return 'B'
        elif average >= 40:
            return 'C'
        else:
            return 'F'

st = input('Enter the name of the student : ')
su = int(input('Enter the subject : '))
mr = []
for i in range(su):
    mark = int(input(f"Enter student subject {i+1} marks = "))
    mr.append(mark)
s = Students(st, mr)
print('Student avrage marks = ', s.average())
print('Student grade = ', s.grade())

# INHERITANCE (Exercises 8–10)
# 8. Person → Employee
# Class Person : name , age
# Class Employee inherits Person , adds emp_id , salary
# Method display_info() shows all details

class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def display(self):
        print('Name = ',self.name)
        print('Age = ',self.age)

class Employee(Person):
    def __init__(self, name, age, emp_id, salary):
        super().__init__(name, age)
        self.emp_id = emp_id
        self.salary = salary

    def display(self):
        super().display()
        print('Employee ID = ',self.emp_id)
        print('Salary = ',self.salary)

name = input("Enter your name: ")
age = int(input("Enter your age: "))
emp_id = input("Enter your employee ID: ")
salary = int(input("Enter your salary: "))
e = Employee(name, age, emp_id, salary)
e.display()

# 9. Vehicle → Car , Bike
# Base Class: Vehicle(name, wheels)
# Subclasses:
# Car : additional attribute fuel_type
# Bike : additional attribute is_geared
# Override a method description() in both

class Vehicle:
    def __init__(self, name, wheels):
        self.name = name
        self.wheels = wheels

    def describe(self):
        return f"{self.name} with {self.wheels} wheels"

class Car(Vehicle):
    def __init__(self, name, wheels, fuel_type):
        super().__init__(name, wheels)
        self.fuel_type = fuel_type

    def describe(self):
        return f"car: {self.name}, Wheels: {self.wheels}, fuel type: {self.fuel_type}"

class Bike(Vehicle):
    def __init__(self, name, wheels, is_geared):
        super().__init__(name, wheels)
        self.is_geared = is_geared

    def describe(self):
        return f"Bike: {self.name}, Wheels: {self.wheels}, Geared: {self.is_geared}"

car = Car("Honda City", 4, "Petrol")
bike = Bike("Hero", 2, True)
print(car.describe())
print(bike.describe())

# 10. Polymorphism with Animals
# Base class Animal with method speak()
# Subclasses Dog , Cat , Cow override speak() with unique sounds
# Call speak() on each object in a loop

class Animal:
    def speak(self):
        pass

class Dog(Animal):
    def speak(self):
        return "Woof!"

class Cat(Animal):
    def speak(self):
        return "Meow!"

class Cow(Animal):
    def speak(self):
        return "Moo!"

# Example
animals = [Dog(), Cat(), Cow()]
for animal in animals:
    print(animal.speak())
