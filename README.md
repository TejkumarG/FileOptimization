# Project Description

## Added Files
- Separate python scripts for both CSV and Excel creation.
- Total of 3 files added.
  - 10% of CSV expected file.
  - 10% of Excel expected file.
  - 1 CSV expected file.
- Code for two types of execution for both CSV and Excel.

## Execution Details
- **CSV Execution:**
  - 10% without parallel: 0 seconds.
  - 10% with parallel: 1 second.
  - Expected without parallel: 107 seconds.
  - Expected with parallel: 31 seconds.
  - Utilized 5 parallel processes for asynchronous execution.

- **Excel Execution:**
  - 10% without parallel: 31 seconds.
  - 10% with parallel: 12 seconds.

## Notes
- The asynchronous execution for both CSV and Excel was achieved using 5 parallel processes.
- The performance improvements with parallel execution are significant, especially in the case of Excel, where the time was reduced from 31 seconds without parallel to 12 seconds with parallel execution.

## File Structure
- The project now includes the following files:
  - [`create_csv.py`](create_csv.py): This is to create CSV files.
  - [`create_excel.py`](create_excel.py): This is to create EXCEL files.
  - [`loan_data.csv`](loan_data.csv): 10% of CSV expected file.
  - [`loan_data.xlsx`](loan_data.xlsx): 10% of Excel expected file.
  - [`loan_data_large.csv`](loan_data_large.csv): 1 CSV expected file.
  - [`csv_with_out_parallel.py`](csv_with_out_parallel.py): Program to calculate with-out any parallel process.
  - [`excel_with_out_parallel.py`](excel_with_out_parallel.py): Program to calculate with-out any parallel process.
  - [`with_csv.py`](with_csv.py): Program to calculate parallel process.
  - [`with_excel.py`](with_excel.py): Program to calculate parallel process.
  
## Code Modifications
- The code has been updated to include two types of execution for both CSV and Excel, taking advantage of parallel processing to improve performance.

Feel free to reach out if you need more details or have any questions.
