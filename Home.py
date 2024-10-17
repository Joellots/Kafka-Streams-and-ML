import streamlit as st
import pandas as pd
import threading
from kafka import KafkaProducer, KafkaConsumer
from queue import Queue
from time import sleep
from json import dumps, loads
from datetime import datetime
import time
import pickle
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import random
from PIL import Image
from google.cloud import firestore
import firebase_admin
from firebase_admin import credentials, auth
import streamlit_authenticator as stauth


st.set_page_config(
    page_title="Kafka Streaming",
    page_icon=":sparkles:",  # You can also use a URL for an image
    #layout="wide"
)

# Load CSN_map (pre-trained ML model and necessary metadata)
with open('CSN_map.pkl', 'rb') as f:
    CSN_map = pickle.load(f)

model = CSN_map['model']
scaler = CSN_map['scaler']
input_cols = CSN_map['input_cols']
categorical_cols = CSN_map['categorical_cols']
numeric_cols = CSN_map['numeric_cols']

# Global variables for managing producer/consumer states
producer_running = False
consumer_running = False
produced_data = []
consumed_data = []

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


# Kafka producer
producer = KafkaProducer(bootstrap_servers=['okore-joel:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Kafka consumer
consumer = KafkaConsumer('demo_test',
                         bootstrap_servers=['okore-joel:9092'],
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

# Function to simulate the Kafka producer
def start_producer():
    global producer_running
    df = pd.read_csv('credit_card_transactions.csv')

    fraud = df[df['is_fraud'] == 1]
    non_fraud = df[df['is_fraud'] == 0]

    # Downsample majority class (non_fraud) to match minority class (fraud)
    non_fraud_sample = non_fraud.sample(n=len(fraud), random_state=42)

    # Combine both minority class and downsampled majority class
    n = random.randint(2, 100)
    df_balanced = pd.concat([fraud, non_fraud_sample])
    data = df_balanced.sample(n=10, random_state=n).reset_index(drop=True)

    for index, row in data.drop('is_fraud', axis=1).iterrows():
        if not producer_running:
            break
        row_dict = row.to_dict()
        row_dict['sent_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # Send transaction to Kafka topic
        producer.send('demo_test', value=row_dict)

        first_name = data.iloc[index]['first']
        last_name = data.iloc[index]['last']
        trans_num = data.iloc[index]['trans_num']

        emojis = ["📦", "🔒", "🚀", "🌍", "📊"]

        if data.iloc[index]['is_fraud'] == 1:
            result = f"""\n{random.choice(emojis)} *[{row_dict['sent_timestamp']}]* Transaction for **{first_name.upper()} {last_name.upper()}**  |  *Transaction ID: {trans_num}*
            """
        else:
            result = f"""\n{random.choice(emojis)} *[{row_dict['sent_timestamp']}]* Transaction for **{first_name.upper()} {last_name.upper()}**  |  *Transaction ID: {trans_num}*
            """

        for word in result.split(" "):
            yield word + " "
            time.sleep(0.02)
        time.sleep(1)  # Simulate real-time streaming  
    producer.flush()

# Function to simulate the Kafka consumer
def start_consumer():
    global consumer_running

    for message in consumer:
        if not consumer_running:
            break
        data = pd.DataFrame([message.value])  # Convert received data to DataFrame
                
        received_timestamp = datetime.now()
        sent_timestamp = datetime.strptime(data['sent_timestamp'].values[0], '%Y-%m-%d %H:%M:%S.%f')
        time_difference = received_timestamp - sent_timestamp

        # Feature Engineering and Prediction
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

        first_name = data.iloc[0]['first']
        last_name = data.iloc[0]['last']
        trans_num = data.iloc[0]['trans_num']

        prediction = model.predict(data[input_cols])

        result_con= ""
        if prediction == 1:
            result_con = f"""\n🔴 [{received_timestamp}] Prediction for **{first_name.upper()} {last_name.upper()}**  |  Transaction: {trans_num}  |  **TRANSACTION DECLINED** (Processed {time_difference.total_seconds()} secs after production)
            """
        else:
            result_con = f"""\n🟢 [{received_timestamp}] Prediction for **{first_name.upper()} {last_name.upper()}**  |  Transaction: {trans_num}  |  **TRANSACTION ACCEPTED** (Processed {time_difference.total_seconds()} secs after production)
            """

        data.to_csv('output.csv', index=False)

        for word in result_con.split(" "):
            yield word + " "
            time.sleep(0.02)
        time.sleep(1)


def get_creds(db):
    cred_ref = db.collection("credentials")
    usernames_ref = cred_ref.document('usernames').collections()
    creds = {"usernames": {}}
    for username_col in usernames_ref:
        for doc in username_col.stream():
            creds["usernames"][doc.id] = doc.to_dict()
    return creds

cred_path = 'anomaly-detection-d4b91-firebase-adminsdk-lwlgg-d92f4bd41c.json'
db = firestore.Client.from_service_account_json(cred_path)

# Load credentials
creds = get_creds(db)

# Initialize Authenticator
authenticator = stauth.Authenticate(
    creds,
    "anomaly_cookie",
    "anomaly_key",
    10,
    "okorejoellots@gmail.com",
)

name, authentication_status, username = authenticator.login(
      'main', 'Enter your username and password', 
      fields={'Form name': 'Login', 'Username':'Username', 'Password':'Password', 'Login':'Login'})

if authentication_status == False:
    st.error('The username/password is incorrect')
if authentication_status == None:
    st.warning('Please enter your username and password')
if authentication_status:

    st.sidebar.image("logo.png", width=100)

    emojis = ["🌟", "😊", "🎉", "👋", "🙌"]
    st.sidebar.title(f'Welcome {name.title()} {random.choice(emojis)} ')
    authenticator.logout('Logout', 'sidebar')
       
    # Streamlit UI
    st.title('REAL-TIME DATA ANALYTICS WITH KAFKA STREAMS & ML')
    st.header('Simulate Data Production and Consumption', divider='rainbow')

    # Display Image
    webp_image = Image.open("image.png")
    image = webp_image.convert("RGB")
    st.image(image, caption="Kafka + ML", width=800)

    project_description = """
    ### Project Title: Real-Time Data Analytics with Kafka

    **Overview:**  
    Welcome to my Real-Time Data Analytics project powered by Apache Kafka! This initiative combines the powerful capabilities of Kafka with machine learning to facilitate quick, informed decision-making through seamless data flow.

    **What You'll Experience:**  
    In this interactive application, you'll see simulated credit card transactions come to life. As these transactions are generated, my machine learning model analyzes them in real time, predicting whether each one is legitimate or potentially fraudulent.

    **Key Features:**
    - **Data Production:** Experience the simulation of credit card transactions that reflect real-world scenarios.
    - **Instant Feedback:** Receive immediate updates on transaction statuses as my Kafka setup processes data in real time.
    - **Engaging Interface:** Enjoy a user-friendly Streamlit interface that makes data visualization both informative and fun.

    Join me in exploring how real-time data streaming and intelligent analytics can revolutionize decision-making in an ever-changing landscape!
    """

    # Display the project description
    st.markdown(project_description)

    if username != 'admin':
        # Producer Controls
        with st.expander("Simuate Data Production"):
            if st.button('Start Producer'):
                if not producer_running:
                    producer_running = True
                    st.write_stream(start_producer)
        
    if username == 'admin':
        # Consumer Controls
        with st.expander("Simuate Data Consumption and ML Analysis"):
            if st.button('Start Consumer'):
                while not consumer_running:
                    consumer_running = True
                    st.write_stream(start_consumer)


        