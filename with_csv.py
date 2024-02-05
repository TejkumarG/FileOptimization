import concurrent.futures

import pandas as pd
import time


def process_chunk(data_frame, desired_date):
    # Convert 'principal' and 'interest' columns to numeric
    data_frame['principal'] = pd.to_numeric(data_frame['principal'], errors='coerce')
    data_frame['interest'] = pd.to_numeric(data_frame['interest'], errors='coerce')

    # Create a boolean column based on the condition
    data_frame['date_condition'] = data_frame['due_date'].str[:7] == desired_date

    # Filter the DataFrame based on the condition
    filtered_df = data_frame[data_frame['date_condition']]

    # Calculate the sum of 'principal' and 'interest' columns
    return filtered_df['principal'].sum() + filtered_df['interest'].sum()


def read_file(chunk_size, total, year_month):
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        futures = []
        result = 0

        header_df = pd.read_csv('loan_data.csv', nrows=1)

        for skip in range(0, total, chunk_size):
            # Submit tasks to read chunks of CSV file
            future = executor.submit(read_csv_chunk, skip, chunk_size, header_df, year_month)
            futures.append(future)

        # # Sum up results from chunk processing
        for future in concurrent.futures.as_completed(futures):
            result += future.result()

    return result


def read_csv_chunk(skip, chunk_size, header_df, year_month):
    # Read a chunk of data from CSV
    try:
        chunk_df = pd.read_csv('loan_data_large.csv', skiprows=skip, nrows=chunk_size, header=None)
    except pd.errors.EmptyDataError:
        print(f"No data found in chunk starting at row {skip}. Skipping...")
        return 0
    # Set columns to match header

    chunk_df.columns = header_df.columns

    # Process the chunk and return the result
    return process_chunk(chunk_df, year_month)


if __name__ == '__main__':
    stared = int(time.time())
    answer = read_file(chunk_size=100000, total=10000001, year_month='2001-01')

    ended = int(time.time())
    print(f'Total - {answer}, Time - {ended - stared} sec')
