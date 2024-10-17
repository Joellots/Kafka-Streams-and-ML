import streamlit as st
import pandas as pd
import threading
from kafka import KafkaProducer, KafkaConsumer
from time import sleep
from json import dumps, loads
from datetime import datetime
import time
import pickle
import numpy as np

# Load CSN_map (pre-trained ML model and necessary metadata)
with open('CSN_map.pkl', 'rb') as f:
    CSN_map = pickle.load(f)

model = CSN_map['model']
scaler = CSN_map['scaler']
input_cols = CSN_map['input_cols']
categorical_cols = CSN_map['categorical_cols']
numeric_cols = CSN_map['numeric_cols']

# Global variables for managing producer/consumer states
producer_thread = None
consumer_thread = None
producer_running = False
consumer_running = False
produced_data = []
consumed_data = []

# Kafka producer
producer = KafkaProducer(bootstrap_servers=['okore-joel:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Kafka consumer
consumer = KafkaConsumer('demo_test',
                         bootstrap_servers=['okore-joel:9092'],
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

# Haversine formula to calculate distance between two lat/long points in miles
def haversine(lat1, lon1, lat2, lon2):
    # Radius of the Earth in miles
    R = 3958.8
    # Convert degrees to radians
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

# Vectorized calculation of distances
def calculate_distance_vectorized(data):
    data['distance'] = haversine(data['lat'], data['long'], data['merch_lat'], data['merch_long'])
    return data


# Function to simulate the Kafka producer
def start_producer(data):
    global producer_running, produced_data
    for index, row in data.drop('is_fraud', axis=1).iterrows():
        if not producer_running:
            break
        # Add a timestamp to the transaction
        row_dict = row.to_dict()
        row_dict['sent_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # Send transaction to Kafka topic
        producer.send('demo_test', value=row_dict)

        # Collect produced data to display in Streamlit
        produced_data.append(row_dict)

        time.sleep(1)  # Simulate real-time streaming by adding a delay
    producer.flush()

# Function to simulate the Kafka consumer
def start_consumer():
    global consumer_running, consumed_data

    placeholder = st.empty()  # Create a placeholder for Streamlit updates

    for message in consumer:
        if not consumer_running:
            break
        data = pd.DataFrame([message.value])  # Convert received data to DataFrame
        received_timestamp = datetime.now()

        # Collect consumed data to display in Streamlit
        #consumed_data.append(message.value)

        # Feature Engineering, prediction, and display results
        sent_timestamp = datetime.strptime(data['sent_timestamp'].values[0], '%Y-%m-%d %H:%M:%S.%f')
        time_difference = received_timestamp - sent_timestamp

        data['trans_date_trans_time'] = pd.to_datetime(data['trans_date_trans_time'])
        data['transaction_hour'] = data['trans_date_trans_time'].dt.hour
        data['transaction_day_of_week'] = data['trans_date_trans_time'].dt.dayofweek
        data['transaction_day'] = data['trans_date_trans_time'].dt.day

        # Preprocess the consumed data for prediction
        data['amt'] = data['amt'].fillna(70.35103545607033)
        data['merch_zipcode'] = data['merch_zipcode'].fillna(45860.0)
        data['category'] = data['category'].fillna('gas_transport')
        data['merchant'] = data['merchant'].fillna('fraud_Kilback LLC')
        data['job'] = data['job'].fillna('Film/video editor')
        data['state'] = data['state'].fillna('TX')
        
         # Label (Gender) Encoder
        data['gender'] = data['gender'].map({'M': 1, 'F': 0})

        # One-hot encoding for categorical columns
        data = pd.get_dummies(data, columns=categorical_cols, drop_first=True)

        # Feature Engineering - age
        data['dob'] = pd.to_datetime(data['dob'])
        data['age'] = (pd.to_datetime('today') - data['dob']).dt.days // 365

        # Feature Engineering - Haversine
        data = calculate_distance_vectorized(data)
        
        # Scaling
        data[numeric_cols] = scaler.transform(data[numeric_cols])

        # Getting User Metadata
        first_name = data.loc[0, 'first']
        last_name = data.loc[0, 'last']
        trans_num = data.loc[0, 'trans_num']

        # Make predictions using the pre-trained model
        prediction = model.predict(data[input_cols])

        # Display the prediction result in Streamlit
        result = ""
        if prediction == 1:
            result = f"[{received_timestamp}] Prediction for {first_name} {last_name} || Transaction: {trans_num} || TRANSACTION DECLINED (Processed {time_difference.total_seconds()} secs after production)\n"
        else:
            result = f"[{received_timestamp}] Prediction for {first_name} {last_name} || Transaction: {trans_num} || TRANSACTION ACCEPTED (Processed {time_difference.total_seconds()} secs after production)\n"
        
        # Update the UI from the main thread
        placeholder.write(result)


# Streamlit UI
st.title('Kafka Streaming App')

# Producer Control
if st.button('Start Producer'):
    if not producer_running:
        producer_running = True
        data = pd.read_csv('credit_card_transactions.csv')  # Load your data
        fraud = data[data['is_fraud'] == 1]
        non_fraud = data[data['is_fraud'] == 0]
        non_fraud_sample = non_fraud.sample(n=len(fraud), random_state=42)
        df_balanced = pd.concat([fraud, non_fraud_sample])
        df = df_balanced.sample(frac=1, random_state=42).reset_index(drop=True)
        
        # Start producer thread
        producer_thread = threading.Thread(target=start_producer, args=(df,))
        producer_thread.start()

if st.button('Stop Producer'):
    producer_running = False
    if producer_thread:
        producer_thread.join()

# Consumer Control
if st.button('Start Consumer'):
    if not consumer_running:
        consumer_running = True
        # Start consumer thread
        consumer_thread = threading.Thread(target=start_consumer)
        consumer_thread.start()

if st.button('Stop Consumer'):
    consumer_running = False
    if consumer_thread:
        consumer_thread.join()

# Display Produced Data
st.subheader('Produced Data')
if produced_data:
    produced_df = pd.DataFrame(produced_data)
    st.dataframe(produced_df)

# Display Consumed Data
st.subheader('Consumed Data')
if consumed_data:
    consumed_df = pd.DataFrame(consumed_data)
    st.dataframe(consumed_df)
