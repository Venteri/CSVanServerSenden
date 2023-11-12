import os
import json
import time
import requests

# Function to format the timestamp
def format_timestamp():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

# Function to check if a CSV file has already been uploaded
def is_already_uploaded(file_name):
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as log_file:
            uploaded_files = log_file.read().splitlines()
            return file_name in uploaded_files
    return False

# Function to log an uploaded CSV file
def log_uploaded_file(file_name):
    with open(log_file_path, 'a') as log_file:
        log_file.write(file_name + '\n')

# Function to authenticate and retrieve the bearer token and user ID
def authenticate(username, password):
    auth_url = f"{server_address}/api/auth/signin"
    while True:
        try:
            response = requests.post(auth_url, json={"username": username, "password": password})
            if response.status_code == 200:
                auth_data = response.json()
                token = auth_data.get('token')
                user_id = auth_data.get('id')
                print(f"Authenticated as {username}.")
                return token, user_id
            else:
                print(f"Authentication error. Status code: {response.status_code}. Retrying in 10 seconds.")
        except requests.exceptions.RequestException as e:
            print(f"Network error: {e}. Retrying in 10 seconds.")
        time.sleep(10)


# Load the configuration data from the config.json file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

server_address = config.get('serverAddress')
username = config.get('username')
password = config.get('password')
folder_to_watch = config.get('folderToWatch')  # Path to the folder to watch from the configuration
log_file_path = 'uploaded_files.txt'  # Path to the text file logging uploaded CSV files
time_sleep = config.get('timeSleep', 60)  # Default to 60 seconds if not set

# Attempt to authenticate with the server
token, user_id = authenticate(username, password)

# Monitor the folder for new CSV files
while True:
    # List all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_to_watch) if f.endswith('.csv')]
    
    # Send files that have not been uploaded yet
    for csv_file in csv_files:
        if not is_already_uploaded(csv_file):
            file_path = os.path.join(folder_to_watch, csv_file)
            upload_url = f"{server_address}/api/files/Csv?userId={user_id}"
            headers = {'Authorization': f'Bearer {token}'}
            
            with open(file_path, 'rb') as file:
                response = requests.post(upload_url, files={'file': file}, headers=headers)
                if response.status_code == 200:
                    log_uploaded_file(csv_file)  # Log the uploaded file
                    print(f"Uploaded: {csv_file}")
                    time.sleep(10)  # Wait for 10 seconds before sending the next file
                else:
                    print(f"Error uploading {csv_file}. Status code: {response.status_code}")
    
    # Wait for 60 seconds before checking the folder again
    time.sleep(time_sleep)
