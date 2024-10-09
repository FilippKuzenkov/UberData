import io
import pandas as pd
import requests

# Function to load and preprocess the DataFrame
def load_data(url):
    response = requests.get(url)
    df = pd.read_csv(io.StringIO(response.text), sep=',', low_memory=False)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df

# Function to create a dimension DataFrame with datetime components
def create_datetime_dim(df):
    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    for prefix in ['pick', 'drop']:
        datetime_dim[f'{prefix}_hour'] = datetime_dim[f'tpep_{prefix}up_datetime'].dt.hour
        datetime_dim[f'{prefix}_day'] = datetime_dim[f'tpep_{prefix}up_datetime'].dt.day
        datetime_dim[f'{prefix}_month'] = datetime_dim[f'tpep_{prefix}up_datetime'].dt.month
        datetime_dim[f'{prefix}_year'] = datetime_dim[f'tpep_{prefix}up_datetime'].dt.year
        datetime_dim[f'{prefix}_weekday'] = datetime_dim[f'tpep_{prefix}up_datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index
    return datetime_dim[['datetime_id', 'tpep_pickup_datetime'] +
                         [f'{prefix}_{time}' for prefix in ['pick', 'drop'] for time in ['hour', 'day', 'month', 'year', 'weekday']]]

# Function to create a dimension DataFrame from unique values
def create_dimension(df, column_name, id_prefix):
    dimension = df[[column_name]].drop_duplicates().reset_index(drop=True)
    dimension[f'{id_prefix}_id'] = dimension.index
    return dimension[[f'{id_prefix}_id', column_name]]

# Function to create rate code dimension
def create_rate_code_dim(df, column_name, rate_code_dict):
    rate_code_dim = df[[column_name]].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim[column_name].map(rate_code_dict)
    return rate_code_dim[['rate_code_id', column_name, 'rate_code_name']]

# Main script
url = 'https://storage.googleapis.com/uber-data-engineering-project/uber_data.csv'
df = load_data(url)

# Create dimension tables
datetime_dim = create_datetime_dim(df)
passenger_count_dim = create_dimension(df, 'passenger_count', 'passenger_count')
trip_distance_dim = create_dimension(df, 'trip_distance', 'trip_distance')

# Rate code mapping
rate_code_dict = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride"
}
rate_code_dim = create_rate_code_dim(df, 'RatecodeID', rate_code_dict)

# Create pickup location dimension
pickup_location_dim = create_dimension(df[['pickup_longitude', 'pickup_latitude']],
                                       ['pickup_longitude', 'pickup_latitude'], 'pickup_location')

# Create dropoff location dimension
dropoff_location_dim = create_dimension(df[['dropoff_longitude', 'dropoff_latitude']],
                                         ['dropoff_longitude', 'dropoff_latitude'], 'dropoff_location')

# Create payment type dimension
payment_type_dict = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}
payment_type_dim = create_rate_code_dim(df, 'payment_type', payment_type_dict)

# Create fact_table by merging all dimensions
fact_table = df.merge(passenger_count_dim, on='passenger_count') \
               .merge(trip_distance_dim, on='trip_distance') \
               .merge(rate_code_dim, on='RatecodeID') \
               .merge(pickup_location_dim, on=['pickup_longitude', 'pickup_latitude']) \
               .merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude']) \
               .merge(datetime_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime']) \
               .merge(payment_type_dim, on='payment_type') \
               [['VendorID', 'datetime_id', 'passenger_count_id', 'trip_distance_id', 'rate_code_id',
                 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
                 'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
                 'tolls_amount', 'improvement_surcharge', 'total_amount']]

# Display the fact table
print(fact_table.head())
