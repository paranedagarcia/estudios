import streamlit as st
import paramiko
import pandas as pd
import toml
from datetime import datetime, timedelta

# Cargar configuración desde secrets.toml
with open(".streamlit/secrets.toml", "r") as f:
    config = toml.load(f)

# Parámetros de conexión
hostname = config['sftp']['hostname']
username = config['sftp']['username']
password = config['sftp']['password']

# Conectar al servidor SFTP
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=hostname, username=username, password=password)
sftp = client.open_sftp()

# Obtener la fecha de ayer
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

st.title("Comparación de archivos CSV")

# Obtener lista de archivos CSV en la raíz
files = sftp.listdir()
csv_files = [f for f in files if f.endswith('.csv')]
st.write(f"Archivos a procesar: {csv_files}")

# Crear DataFrame para almacenar resultados
df = pd.DataFrame(columns=['Nombre de archivo', 'Tamaño en KB hoy',
                  'Tamaño en KB ayer', 'Cantidad de filas hoy', 'Cantidad de filas ayer'])

# Iterar sobre cada archivo CSV
for file in csv_files:
    st.write(f"Procesando archivo {file}")
    # Obtener tamaño y cantidad de filas del archivo actual
    sftp.chdir('/')
    stats = sftp.stat(file)
    size_today = stats.st_size / 1024
    with sftp.open(file) as f:
        rows_today = sum(1 for line in f)
    st.write(f"Archivo {file} procesado. Tamaño: {
             size_today} KB, Filas: {rows_today}")

    # Obtener tamaño y cantidad de filas del archivo de ayer
    sftp.chdir(yesterday_str)
    try:
        stats = sftp.stat(file)
        size_yesterday = stats.st_size / 1024
        with sftp.open(file) as f:
            rows_yesterday = sum(1 for line in f)
    except FileNotFoundError:
        size_yesterday = None
        rows_yesterday = None

    # Agregar fila al DataFrame
    df = df.append({'Nombre de archivo': file, 'Tamaño en KB hoy': size_today,
                   'Tamaño en KB ayer': size_yesterday, 'Cantidad de filas hoy': rows_today,
                    'Cantidad de filas ayer': rows_yesterday}, ignore_index=True)

# Cerrar conexión SFTP
sftp.close()
client.close()

# Mostrar DataFrame en Streamlit
st.dataframe(df)
