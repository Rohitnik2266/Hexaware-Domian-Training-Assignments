# VARIABLES, DATA TYPES, OPERATORS
# 1. Digit Sum Calculator Ask the user for a number and calculate the sum of its digits. Example: 753 → 7 + 5 + 3 = 15

num = input("Enter a number: ")
sum = 0
for i in num:
    sum += int(i)
print('Sum of Number = ', sum)

# 2. Reverse a 3-digit Number Input a 3-digit number and print it reversed. Input: 123 → Output: 321

num2 = input("Enter a number: ")
rev_num = num2[::-1]
print('The Reverse Number = ', rev_num)

# 3. Unit Converter Build a converter that takes meters and converts to:
# centimeters
# feet
# inches

meter = float(input("Enter meters: "))
print('Centimeters = ', meter*100)
print('Feet = ', meter*3.28084)
print('inches = ', meter*39.3701)

# 4. Percentage Calculator Input marks of 5 subjects and calculate total, average, and percentage. Display grade based on the percentage.

marks1 = float(input('Enter marks in subject1 = '))
marks2 = float(input('Enter marks in subject2 = '))
marks3 = float(input('Enter marks in subject3 = '))
marks4 = float(input('Enter marks in subject4 = '))
marks5 = float(input('Enter marks in subject5 = '))
totalmarks = marks1 + marks2 + marks3 + marks4 + marks5
avgmarks = totalmarks/5
percentmarks = (totalmarks/500)*100
print('Total Marks = ', totalmarks)
print('Average Marks = ',avgmarks)
print('Percentage = ', percentmarks)
if percentmarks >= 90:
    print('Grade = A')
elif percentmarks >= 80:
    print('Grade = B')
elif percentmarks >= 70:
    print('Grade = C')
elif percentmarks >= 60:
    print('Grade = D')
elif percentmarks >= 50:
    print('Grade = E')
else:
    print('Grade = F')

# CONDITIONALS
# 5. Leap Year Checker A year is a leap year if it’s divisible by 4 and (not divisible by 100 or divisible by 400).

year = int(input('Enter year: '))
if year % 4 == 0 and year % 100 != 0 or year % 400 == 0:
    print('It is leap year')
else:
    print('It is not leap year')

# 6. Simple Calculator Input two numbers and an operator ( + - * / ) and perform the operation using if...elif...else .

num1 = int(input('Enter 1st number: '))
num2 = int(input('Enter 2nd number: '))
op = input('Enter operation( + - * / ): ')
if op == '+':
    print('Addition =',num1+num2)
elif op == '-':
    print('Subtraction =',num1-num2)
elif op == '*':
    print('Multiplication =',num1*num2)
elif op == '/':
    print('Division =',num1/num2)
else:
    print('Invalid operation')

# 7. Triangle Validator Given 3 side lengths, check whether they can form a valid triangle.

sides = [float(input(f'Enter sides {i+1}: ')) for i in range(3)]
if sides[0] + sides[1] > sides[2] and sides[0] + sides[2] > sides[1] and sides[1] + sides[2] > sides[0]:
    print('Vaild Triangle')
else:
    print('Invalid triangle')

# 8. Bill Splitter with Tip Ask total bill amount, number of people, and tip percentage. Show final amount per person.

bill = float(input('Enter bill: '))
ppl = int(input('Enter Number of people: '))
tip = float(input('Enter Tip: '))
totalbill = bill + (bill*tip/100)
perppl = totalbill/ppl
print('Total bill = ', perppl)

# LOOPS
# 9. Find All Prime Numbers Between 1 and 100 Use a nested loop to check divisibility.
for num in range(2, 101):
    is_prime = True
    for i in range(2, int(num**0.5)+1):
        if num % i == 0:
            is_prime = False
            break
    if is_prime:
        print(num, end=' ')

# 10. Palindrome Checker Ask for a string and check whether it reads the same backward.

text = input("Enter a string: ")
if text == text[::-1]:
    print("Palindrome")
else:
    print("Not a palindrome")

# 11. Fibonacci Series (First N Terms) Input n , and print first n terms of the Fibonacci sequence.

n = int(input("Enter n: "))
a, b = 0, 1
for _ in range(n):
    print(a, end=' ')
    a, b = b, a + b

# 12. Multiplication Table (User Input) Take a number and print its table up to 10:
# 5 x 1 = 5
# 5 x 2 = 10
# ...

num = int(input("Enter a number: "))
for i in range(1, 11):
    print(f"{num} x {i} = {num * i}")

# 13. Number Guessing Game
# Generate a random number between 1 to 100
# Ask the user to guess
# Give hints: "Too High", "Too Low"

import random
secret = random.randint(1, 100)

while True:
    guess = int(input("Guess the number (1-100): "))
    if guess < secret:
        print("Too Low")
    elif guess > secret:
        print("Too High")
    else:
        print("Correct")
        break

# Loop until the correct guess
# 14. ATM Machine Simulation
# Balance starts at 10,000
# Menu: Deposit / Withdraw / Check Balance / Exit
# Use a loop to keep asking
# Use conditionals to handle choices

balance = 10000

while True:
    print("\n1. Deposit\n2. Withdraw\n3. Check Balance\n4. Exit")
    choice = input("Choose an option: ")

    if choice == '1':
        amount = float(input("Enter deposit amount: "))
        balance += amount
    elif choice == '2':
        amount = float(input("Enter withdraw amount: "))
        if amount <= balance:
            balance -= amount
        else:
            print("Insufficient balance")
    elif choice == '3':
        print("Current balance: ₹", balance)
    elif choice == '4':
        print("Exiting...")
        break
    else:
        print("Invalid option")

# 15. Password Strength Checker
# Ask the user to enter a password
# Check if it's at least 8 characters
# Contains a number, a capital letter, and a symbol

import re
password = input("Enter password: ")

if (len(password) >= 8 and
    re.search(r"[A-Z]", password) and
    re.search(r"[0-9]", password) and
    re.search(r"[\W_]", password)):
    print("Strong password")
else:
    print("Weak password")

# 16. Find GCD (Greatest Common Divisor)
# Input two numbers
# Use while loop or Euclidean algorithm

a = int(input("Enter first number: "))
b = int(input("Enter second number: "))

while b:
    a, b = b, a % b

print("GCD is:", a)
