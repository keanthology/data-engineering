%python
# display
message = "hello"
print(message)

### LIST DATASETS ###

# List datasets under the specified directory
datasets = dbutils.fs.ls('/mnt/davidbrxdatalakedev/staging/JDA/HM/')

# Display the datasets
display(datasets)

### LIST DATA POINTS ###

# Define the table location
table_path = "/mnt/davidbrxdatalakedev/staging/JDA/TV/vendor/"

# Read the data to get the column names
df = spark.read.format('delta').load(table_path)

# Get the column names
columns = df.columns

# Print the column names in a readable format
for column in columns:
    print(column)

### LIST COLUMN NAMES ###
# Define the table location
table_path = "/mnt/davidbrxdatalakedev/staging/JDA/HM/tender_BIN/"

# Read the data to get the column names
df = spark.read.format('delta').load(table_path)

# Get the column names
columns = df.columns

# Print the column names
print("Column Names:", columns)
