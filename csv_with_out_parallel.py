from threading import Thread

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


def read_file():
    # Read all files in chuck manner
    stared = int(time.time())
    print('Started')
    total = 10000001
    chunk_size = 100000

    answer = 0

    header_df = pd.read_csv('loan_data.csv', nrows=1)

    for skip in range(1, total, chunk_size):
        try:
            start = int(time.time())
            df = pd.read_csv('loan_data.csv', skiprows=skip, nrows=chunk_size, header=None)
            df.columns = header_df.columns
            end = int(time.time())
            print('Inside time - ', end - start)
            print()
            answer += process_chunk(df, '2001-01')
        except pd.errors.EmptyDataError:
            print(f"No data found in chunk starting at row {skip}. Skipping...")
            continue

    ended = int(time.time())
    print('\nEnded')
    print(f'Total - {answer}, Time - {ended-stared} sec')


if __name__ == '__main__':
    read_file()
