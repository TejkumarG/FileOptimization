from threading import Thread

import pandas as pd
import time


def calculate_list(data_frame, desired_date):
    # Create a boolean column based on the condition
    data_frame['date_condition'] = data_frame['due_date'].map(lambda x: x[:7] == desired_date)

    # Filter the DataFrame based on the condition
    filtered_df = data_frame[data_frame['date_condition']]

    # Drop the temporary boolean column
    filtered_df = filtered_df.drop('date_condition', axis=1)

    # filtered_df.sum('principal') + filtered_df.sum('interest')

    return filtered_df['principal'].sum() + filtered_df['interest'].sum()


def read_file():
    # Read all files in chuck manner
    stared = int(time.time())
    print('Started')
    total = 144001
    chunk_size = 10000

    answer = 0

    header_df = pd.read_excel('loan_data.xlsx', nrows=1)

    for skip in range(1, total, chunk_size):
        start = int(time.time())
        df = pd.read_excel('loan_data.xlsx', skiprows=skip, nrows=chunk_size)
        df.columns = header_df.columns
        # print(df.iloc[-1])
        end = int(time.time())
        print('Inside time - ', end - start)
        print()
        answer += calculate_list(df, '2001-01')

    ended = int(time.time())
    print('\nEnded')
    print(f'Total - {answer}, Time - {ended-stared} sec')


if __name__ == '__main__':
    read_file()
