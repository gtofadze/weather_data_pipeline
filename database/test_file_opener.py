
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, 'db_details.txt')

#file_path = Path(__file__).with_name('db_details.txt')

#file_path = "db_details.txt"

print(file_path, '='*50)

try:
    with open(file_path, "r") as file:
        lines = file.readlines()
except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")

cleared_lines = [line.strip() for line in lines]

print(cleared_lines)