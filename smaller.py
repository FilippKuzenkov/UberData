import pandas as pd

# Load the large CSV file
file_path = 'tripdata_july_2024.csv'  # Replace with your actual file path
df = pd.read_csv(file_path, low_memory=False)

# Print the actual column names to confirm
print("Column names in the CSV file:")
print(df.columns.tolist())

# Specify the columns you want to check for non-empty values
columns_to_check = [
    'VendorID',
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime',
    'passenger_count',
    'trip_distance',
    'RatecodeID',
    'store_and_fwd_flag',
    'PULocationID',
    'DOLocationID',
    'payment_type',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'congestion_surcharge',
    'Airport_fee'
]

# Drop rows where any of the specified columns have NaN values
filtered_df = df.dropna(subset=columns_to_check)

# Take a random sample of 150,000 rows
sampled_df = filtered_df.sample(n=150000, random_state=1)

# Save the sampled DataFrame to a new CSV file
sampled_df.to_csv('modified_file.csv', index=False)

# Display the final number of rows in the sampled DataFrame
print(f'Total rows in sampled data: {sampled_df.shape[0]}')
