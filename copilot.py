import streamlit as st
import paramiko
import pandas as pd
import os
import toml
from datetime import datetime, timedelta
from io import StringIO


# Load SFTP connection parameters from secrets.toml
secrets = toml.load(os.path.join(".streamlit", "secrets.toml"))
sftp_host = secrets['sftp']['hostname']
sftp_port = secrets['sftp']['port']
sftp_username = secrets['sftp']['username']
sftp_password = secrets['sftp']['password']

# Connect to SFTP server
transport = paramiko.Transport((sftp_host, sftp_port))
transport.connect(username=sftp_username, password=sftp_password)
sftp = paramiko.SFTPClient.from_transport(transport)

# Get list of CSV files in the root directory
csv_files = [f for f in sftp.listdir() if f.endswith('.csv')]

# Get yesterday's date
yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

# Initialize lists to store comparison data
file_names = []
size_today = []
size_yesterday = []
rows_today = []
rows_yesterday = []

# Function to get file size in kilobytes


def get_file_size(file):
    return sftp.stat(file).st_size / 1024


# Function to get number of rows in a CSV file
st.write(f"Archivos a procesar: {csv_files}")


def get_row_count(file):
    with sftp.open(file, 'r') as f:
        return sum(1 for line in f)


# Compare files
for file in csv_files:
    file_names.append(file)
    st.write(f"Procesando archivo {file}")
    # Today's file
    size_today.append(get_file_size(file))
    rows_today.append(get_row_count(file))

    # Yesterday's file
    yesterday_file = os.path.join(yesterday, file)
    if sftp.exists(yesterday_file):
        size_yesterday.append(get_file_size(yesterday_file))
        rows_yesterday.append(get_row_count(yesterday_file))
    else:
        size_yesterday.append(None)
        rows_yesterday.append(None)

# Close SFTP connection
sftp.close()
transport.close()

# Create DataFrame for comparison
data = {
    'Nombre de archivo': file_names,
    'Tamaño en KB hoy': size_today,
    'Tamaño en KB ayer': size_yesterday,
    'Cantidad de filas hoy': rows_today,
    'Cantidad de filas ayer': rows_yesterday
}
df = pd.DataFrame(data)

# Display DataFrame in Streamlit
st.title('Comparación de archivos CSV en SFTP')
st.dataframe(df)
