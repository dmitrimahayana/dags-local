import snowflake.connector
import pandas as pd

# sf_conn = snowflake.connector.connect(
#         schema='FLIGHT_SCHEMA',
#         user='NAINAFI',
#         account='IUDFCYJ-UD33577',
#         warehouse='COMPUTE_WH',
#         database='FLIGHTS'
#     )

# sf_cursor = sf_conn.cursor()
# results = sf_cursor.execute("SELECT * FROM FLIGHTS.FLIGHT_SCHEMA.DUMMY_TABLE").fetchall()
# print(results)

# SNOWFLAKE_SAMPLE_TABLE = 'DUMMY_TABLE'
# SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES (%(id)s, 'ABC', 'X', 1,2,3,4, '2023-09-18', '2023-09-18')"
# SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(5)]
# SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
# print(SQL_MULTIPLE_STMTS)