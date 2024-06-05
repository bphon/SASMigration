

import datetime

file_path = '/Users/jerome/Desktop/GithubRepo/SASMigration/Routine Schedule/task.txt'

# Use 'with' statement to ensure file closing
with open(file_path, 'a') as file:
    # Write the content to the file
    file.write(f'{datetime.datetime.now()} - The script ran\n')  # Added '\n' for newline

# Print a message to indicate successful writing
print("Content has been written to the file.")