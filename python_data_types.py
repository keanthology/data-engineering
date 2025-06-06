# =====================================================
# Python Data Types Demonstration
# Author: [Your Name]
# Description: Examples of basic built-in Python data types.
# =====================================================

# 1. Integer (int): Whole numbers (positive or negative)
age = 25
print("Integer:", age, type(age))  # Output: <class 'int'>

# 2. Float (float): Numbers with decimal points
price = 199.99
print("Float:", price, type(price))  # Output: <class 'float'>

# 3. String (str): Sequence of characters/text
name = "Databricks Learner"
print("String:", name, type(name))  # Output: <class 'str'>

# 4. Boolean (bool): True or False
is_active = True
print("Boolean:", is_active, type(is_active))  # Output: <class 'bool'>

# 5. List (list): Ordered, changeable collection of items
colors = ["red", "green", "blue"]
print("List:", colors, type(colors))  # Output: <class 'list'>

# 6. Tuple (tuple): Ordered, unchangeable collection of items
dimensions = (1920, 1080)
print("Tuple:", dimensions, type(dimensions))  # Output: <class 'tuple'>

# 7. Set (set): Unordered collection of unique items
unique_ids = {101, 102, 103}
print("Set:", unique_ids, type(unique_ids))  # Output: <class 'set'>

# 8. Dictionary (dict): Key-value pairs
user = {
    "name": "Alice",
    "age": 30,
    "role": "Data Engineer"
}
print("Dictionary:", user, type(user))  # Output: <class 'dict'>

# 9. NoneType (None): Represents a null or no value
data = None
print("NoneType:", data, type(data))  # Output: <class 'NoneType'>

# =====================================================
# Bonus: Type Casting
# =====================================================

# Convert string to integer
num_str = "42"
num_int = int(num_str)
print("Type Casting: str to int:", num_int, type(num_int))  # Output: 42 <class 'int'>

# Convert float to integer
price_float = 45.99
price_int = int(price_float)
print("Type Casting: float to int:", price_int, type(price_int))  # Output: 45 <class 'int'>
