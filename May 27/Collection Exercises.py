# LISTS
# 1. List of Squares Create a list of squares of numbers from 1 to 20.

sqr = [i**2 for i in range(1,21)]
print(sqr)

# 2. Second Largest Number Find the second largest number in a list without using sort()

list = [10, 20, 4, 45, 99]
m1 = m2 = float("-inf")

for i in list:
    if i > m1:
        m2 = m1
        m1 = i
    elif m1 > i >m2:
        m2 = i
print(m2)

# 3. Remove Duplicates Write a program to remove all duplicate values from a list while preserving order.

list = [1,2,2,3,4,5,5,6,7,8,9]
l2 = []
seen = set()
for i in list:
    if i not in seen:
        l2.append(i)
        seen.add(i)
print(l2)

# 4. Rotate List Rotate a list to the right by k steps. Example: [1, 2, 3, 4, 5] rotated by 2 → [4, 5, 1, 2, 3]

list = [1,2,3,4,5,6]
a = int(input("enter element of:"))
rl = list[-a:] + list[:-a]
print(rl)

# 5. List Compression From a list of numbers, create a new list with only the even numbers doubled.

list2 = [i*2 for i in list if i%2 == 0]
print(list2)

# TUPLES
# 6. Swap Values Write a function that accepts two tuples and swaps their contents.

t1 = (1,2,3)
t2 = (4,5,6)
print(t1,t2)
t1, t2 = t2, t1
print(t1, t2)

# 7. Unpack Tuples Unpack a tuple with student details: (name, age, branch, grade) and print them in a sentence.

student = ('Rohit', 23, 'cse', 'A')
name, age, branch, grade = student
print(f'Student Name: {name},age : {age},grade: {grade},branch: {branch}')

# 8. Tuple to Dictionary Convert a tuple of key-value pairs into a dictionary.
# Example: (("a", 1), ("b", 2)) → {"a": 1, "b": 2}

tup = (("a", 1), ("b", 2))
print(tup)
dict = dict(tup)
print(dict)

# SETS
# 9. Common Items Find the common elements in two user-defined lists using sets.
list1 = [1, 2, 3, 4]
list2 = [3, 4, 5, 6]
set1 = set(list1)
set2 = set(list2)
print(set1.intersection(set2))

# 10. Unique Words in Sentence Take a sentence from the user and print all unique words.

user = input("Enter a sentance :")
words = set(user.split())
print(words)

# 11. Symmetric Difference Given two sets of integers, print elements that are in one set or the other, but not both.

a = {1, 2, 3}
b = {3, 4, 5}
sys = a ^ b
print(sys)

# 12. Subset Checker Check if one set is a subset of another.

a = {1, 2, 3}
b = {1, 2, 3, 4, 5}
print(a.issubset(b))

# DICTIONARIES
# 13. Frequency Counter Count the frequency of each character in a string using a dictionary.
s = "hello world"
freq = {}
for char in s:
    freq[char] = freq.get(char, 0) + 1

print("Frequency:", freq)

# 14. Student Grade Book Ask for names and marks of 3 students. Then ask for a name and display their grade ( >=90: A , >=75: B , else C).
student = {}
for i in range(3):
    name = input("Enter your name: ")
    marks = int(input("Enter your marks: "))
    if marks >= 80:
        grade = "A"
    elif marks >= 60:
        grade = "B"
    elif marks > 40:
        grade = "C"
    else:
        grade = "D"
    student[name] = grade

ot = input("Enter student name to check grade: ")
print(f"Grade of {ot}: {student.get(ot, 'Not found')}")

# 15. Merge Two Dictionaries Merge two dictionaries. If the same key exists, sum the values.

d1 = {'a': 100, 'b': 200}
d2 = {'a': 300, 'c': 400}

result = d1.copy()
for key, value in d2.items():
    result[key] = result.get(key, 0) + value

print(result)


# 16. Inverted Dictionary Invert a dictionary’s keys and values. Input: {"a": 1, "b": 2} → Output: {1: "a", 2: "b"}

d = {"a": 1, "b": 2}
inverted = {}
for key, value in d.items():
    inverted[value] = key

print( inverted)

# 17. Group Words by Length Input a list of words. Create a dictionary where the key is word length and the value is a list of words of that length.

words = ["hi", "hello", "world", "go", "yes"]
grouped = {}

for word in words:
    length = len(word)
    if length not in grouped:
        grouped[length] = []
    grouped[length].append(word)

print( grouped)
