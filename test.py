import pandas as pd
from datetime import datetime, timedelta
import numpy as np

# Generating synthetic data
num_loans = 5  # You can change this to the number of loans you want
num_repetitions = 3  # Number of times to repeat 'Due date', 'Principal', and 'Interest' for each loan

due_dates = [datetime(2022, 1, 1) + timedelta(days=np.random.randint(1, 30)) for _ in range(num_loans)]

# Create a dictionary with repeated data
data = {
    'Loan Id': [f'Loan_{i+1}' for i in range(num_loans) for _ in range(num_repetitions)],
    'Due date': [date for date in due_dates for _ in range(num_repetitions)],
    'Principal': np.repeat(np.random.uniform(1000, 5000, num_loans), num_repetitions),
    'Interest': np.repeat(np.random.uniform(5, 15, num_loans), num_repetitions)
}

df = pd.DataFrame(data)

# Displaying the DataFrame
print(df)
