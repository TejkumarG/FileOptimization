import dask.dataframe as dd
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from datetime import datetime, timedelta


# Step 1: Read the large file using Dask
def read_large_file(file_path):
    dask_df = dd.read_csv(file_path, sample=2e6)
    return dask_df


# Step 2: Define a function to calculate principal and interest
def calculate_loan_data(row):
    # Your calculation logic based on the row data
    # You can access row['Loan Id'], row['Due date'], row['Principal'], row['Interest']
    # Perform calculations and return the result
    # For example, return row['Principal'] + row['Interest']
    return row['Principal'] + row['Interest']


# Main function to process data and calculate principal and interest
def process_data(start_month_year):
    # Step 1: Read the large file
    file_path = 'loan_data.csv'
    dask_df = read_large_file(file_path)

    # Extract month and year from the start date
    start_date = datetime.strptime(start_month_year, '%Y-%m')

    # Filter data based on the month and year
    dask_df_filtered = dask_df[dask_df['Due date'].dt.month == start_date.month]
    dask_df_filtered = dask_df_filtered[dask_df_filtered['Due date'].dt.year == start_date.year]

    # Step 3: Calculate principal and interest using the defined function
    dask_df_filtered['Total Amount'] = dask_df_filtered.apply(calculate_loan_data, axis=1, meta=('x', 'f8'))

    # Step 4: Compute the Dask DataFrame to Pandas DataFrame
    pandas_df = dask_df_filtered.compute()

    return pandas_df


# Main function to parallelize processing using ThreadPoolExecutor
def main():
    # Set the start date for data filtering (format: 'YYYY-MM')
    start_month_year = '2020-01'

    # Create a ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit the main processing function with different start dates
        futures = [executor.submit(process_data, start_month_year) for _ in range(0, 10)]

        # Wait for all threads to complete
        results = [future.result() for future in futures]

        # Concatenate the results if needed
        final_result = pd.concat(results, ignore_index=True)

    # Do further processing or save the final result as needed
    final_result.to_excel('processed_data.xlsx', index=False, engine='openpyxl')


if __name__ == "__main__":
    main()
