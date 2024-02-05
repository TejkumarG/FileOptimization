import dask.dataframe as dd
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# Function to generate synthetic data for a loan
def generate_loan_data(loan_id):
    np.random.seed(loan_id)
    start_date = datetime(2000, 1, 1)
    end_date = start_date + timedelta(days=365 * 12)

    # Generate random day for each month
    random_day = np.random.randint(1, 28)  # Assume a random day between 1 and 28

    # Generate monthly dates with the same random day
    due_dates = pd.date_range(start=start_date, end=end_date, freq='MS') + timedelta(days=random_day)
    due_dates = due_dates.date

    # Generate random principal and interest values with a small difference
    base_principal = np.random.uniform(1000, 5000)
    principal_variation = np.random.uniform(-100, 100)

    principal = base_principal + principal_variation
    interest = principal * np.random.uniform(0.005, 0.02)  # Interest is a small fraction of principal

    data = {
        'loan_id': [loan_id] * len(due_dates),
        'due_date': due_dates,
        'principal': principal,
        'interest': interest
    }

    return pd.DataFrame(data)


# Generate data for 10 loans
loan_data_list = [generate_loan_data(loan_id) for loan_id in range(1, 5001)]

# Concatenate dataframes into a single Dask dataframe
dask_df = dd.from_pandas(pd.concat(loan_data_list), npartitions=10)

# Sort the Dask dataframe by 'Loan Id' and 'Due date'
dask_df_sorted = dask_df.sort_values(['loan_id', 'due_date'])

# Convert Dask dataframe to Pandas dataframe
pandas_df_sorted = dask_df_sorted.compute()


# Save Pandas dataframe to an Excel file
pandas_df_sorted.to_excel('loan_data.xlsx', sheet_name='loan_data', index=False, engine='openpyxl')

# chunk_size = 500000
#
# # Write each chunk to a separate sheet in Excel
# with pd.ExcelWriter('loan_data.xlsx', engine='openpyxl') as writer:
#     for i in range(len(pandas_df_sorted) // chunk_size):
#         start_idx = i * chunk_size
#         end_idx = min((i + 1) * chunk_size, len(pandas_df_sorted))
#         chunk = pandas_df_sorted.iloc[start_idx:end_idx]
#         sheet_name = 'loan_data_chunk_{}'.format(i)
#         chunk.to_excel(writer, sheet_name=sheet_name, index=False)
