import subprocess
import pandas as pd

# Get the differences between the current branch and the master branch
result = subprocess.run(['git', 'diff', '--name-status', 'master'], stdout=subprocess.PIPE)

# Split the output into lines
lines = result.stdout.decode('utf-8').split('\n')

# Each line is a file difference, split it into the status and the file name
differences = [line.split('\t', 1) for line in lines if line]

# Create a dataframe
df = pd.DataFrame(differences, columns=['Status', 'File'])

# Write to Excel
df.to_excel('git_diff_master(20230627)_v_20230706.xlsx', index=False)
