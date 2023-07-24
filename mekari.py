import pandas as pd
import math
from datetime import datetime
import sqlite3
import os

direc = os.path.dirname(os.path.abspath(__file__))

conn = sqlite3.connect(direc+r'/mekari.db')


def dim_date():
    start_date = '2020-01-01'
    end_date = '2030-12-31'

    # Create a DataFrame with the date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # Create a dictionary to hold the values for each column
    dim_date_dict = {
        'id': date_range.strftime('%Y%m%d').astype(int),
        'the_date': date_range,
        'the_year': date_range.year,
        'day_of_year': date_range.dayofyear,
        'day_of_month': date_range.day,
        'the_day_name': date_range.strftime('%a'),
        'the_day_full_name': date_range.strftime('%A'),
        'the_month': date_range.month,
        'the_month_name': date_range.strftime('%b'),
        'the_month_full_name': date_range.strftime('%B'),
        'ym_id': date_range.strftime('%Y%m').astype(int),
        'the_quarter': pd.PeriodIndex(date_range, freq='Q').strftime('%q').astype(str),
        'the_quarter_name': pd.PeriodIndex(date_range, freq='Q').strftime('Q%q'),
        'the_quarter_full_name': pd.PeriodIndex(date_range, freq='Q').strftime('Quarter%q'),
        'the_week_month7': ((date_range.day) // 7 + 1).astype(int),
        'the_week_month': ((date_range.day - 1) // 7 + 1).astype(int),
        'the_week_year': date_range.strftime('%U').astype(int) + 1,
        'the_week_name': date_range.strftime('Wk ') + (date_range.strftime('%U').astype(int) + 1).astype(str),
        'the_week_full_name': 'Week' + (date_range.strftime('%U').astype(int) + 1).astype(str),
        'ymw_id': (date_range.strftime('%Y%m%U').astype(int) + 1),
        'the_week_month_name': date_range.strftime('%b Wk ') + (date_range.strftime('%U').astype(int) + 1).astype(str),
        'day_of_week_number': ((date_range.weekday + 1) % 7) + 1
    }

    # Create a DataFrame from the dictionary
    dim_date_df = pd.DataFrame(dim_date_dict)

    dim_date_df.to_sql('dim_date', conn, if_exists='replace', index=False)


def fact_salary():
    emp = pd.read_csv(direc + '/employees.csv')
    emp.rename(columns={'employe_id': 'employee_id'}, inplace=True)
    emp['join_date'] = pd.to_datetime(emp['join_date'])
    emp['resign_date'] = pd.to_datetime(emp['resign_date'])

    time = pd.read_csv(direc + '/timesheets.csv')
    time['date'] = pd.to_datetime(time['date'])
    time['checkin'] = pd.to_datetime(time['checkin'])
    time['checkout'] = pd.to_datetime(time['checkout'])
    time['month'] = time['date'].dt.strftime('%m')
    time['year'] = time['date'].dt.strftime('%Y')

    # Calculate working hours per day
    time['working_hours'] = (time['checkout'] - time['checkin']).dt.total_seconds() / 3600

    # Group by employee and date, then calculate the average working hours per day
    average_hours_per_day = time.groupby(['employee_id', 'date'])['working_hours'].mean().reset_index()

    # Calculate the overall average working hours per day
    avg_hours = average_hours_per_day['working_hours'].mean()

    print(average_hours_per_day)
    print("Overall average working hours per day:", avg_hours)

    default_work_hours = math.floor(avg_hours)
    time['working_hours'] = time['working_hours'].fillna(default_work_hours)

    stg_working_hours = pd.merge(emp, time, on='employee_id')
    print(stg_working_hours.columns)
    stg_working_hours.to_sql('stg_working_hours', conn, if_exists='replace', index=False)

    query = """
    SELECT branch_id, employee_id, salary, "month", "year"
    FROM stg_working_hours
    GROUP BY employee_id, branch_id, month, year;
    """

    # Execute the query and put the result into a DataFrame
    stg_salary = pd.read_sql_query(query, conn)
    stg_salary = stg_salary.groupby(['branch_id', 'month', 'year'])['salary'].sum().reset_index()
    print(stg_salary)

    query = """SELECT branch_id, sum(working_hours) total_hours, "month", "year", COUNT(DISTINCT employee_id) total_employee 
    FROM stg_working_hours swh 
    group by branch_id , "month" , "year" ;"""

    # Execute the query and put the result into a DataFrame
    stg_total_hours = pd.read_sql_query(query, conn)
    print(stg_total_hours)

    # Merge the two DataFrames on 'branch_id', 'month', and 'year'
    fact_salary = pd.merge(stg_salary, stg_total_hours, on=['branch_id', 'month', 'year'])

    # Calculate stg_salary['salary'] / stg_total_hours['total_hours']
    fact_salary['salary_per_hour'] = fact_salary['salary'] / fact_salary['total_hours']

    # Convert 'year' and 'month' columns to strings
    fact_salary['year'] = fact_salary['year'].astype(str)
    fact_salary['month'] = fact_salary['month'].astype(str)

    # Convert 'year' and 'month' to periods and get the last date of each period
    fact_salary['dim_date_id'] = pd.to_datetime(fact_salary['year'] + '-' + fact_salary['month'] + '-01')
    fact_salary['dim_date_id'] = fact_salary['dim_date_id'] + pd.offsets.MonthEnd(0)
    fact_salary['dim_date_id'] = fact_salary['dim_date_id'].dt.strftime('%Y%m%d').astype(int)
    fact_salary = fact_salary[['dim_date_id', 'branch_id', 'salary', 'total_hours', 'total_employee', 'salary_per_hour']]

    fact_salary.to_sql('fact_salary', conn, if_exists='replace', index=False)


dim_date()
fact_salary()
