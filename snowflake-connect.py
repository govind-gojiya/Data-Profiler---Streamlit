import snowflake.connector as snow

# Create a connection to Snowflake
connection = snow.connect(
    # url="https://nidvzgj-ui11110.snowflakecomputing.com",
    user="GovindGojiya",
    password="Govind_Gojiya@29",
    account="nidvzgj-ui11110"
)

# Define a cursor
cursor = connection.cursor()

try:
    # Execute a SQL query against Snowflake to get the current_version
    cursor.execute("SELECT current_version()")
    one_row = cursor.fetchone()

    # Display the version information
    print(one_row[0])
finally:
    cursor.close()
connection.close()


