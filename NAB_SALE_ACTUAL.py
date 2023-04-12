import pandas as pd
import pymysql
import pyodbc
from sqlalchemy import create_engine

# Connect to MySQL database
mysql_conn = pymysql.connect(host='Host',
                             user='User',
                             password='Password',
                             db='Database Name',
                             port='port number')

# Connect to SQL Server database using Windows Authentication
mssql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=server;'
    'DATABASE=database_name;'
#    'Trusted_Connection=yes;' If using window Authen
))

tables = [                  # Table to be updated
    'NAB_SALE_ACTUAL',
    'MST_SIS_Product',
    'MST_SIS_SaleOrg',
    'MST_SIS_Customer',
    'MST_SIS_RegionSale'
]

for table in tables:
    # Read data from MySQL table
    mysql_data = pd.read_sql(f'SELECT * FROM {table}', mysql_conn)
    
    # Write data to SQL Server table (replace the table if exists)
    mysql_data.to_sql(table, mssql_engine, if_exists='replace', index=False)

# Close connections
mysql_conn.close()
print("Complete")