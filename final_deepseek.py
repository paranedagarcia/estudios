import streamlit as st
from functools import wraps
import paramiko
from datetime import datetime, timedelta
import stat
import time
import socket
import pandas as pd

from funciones import menu_pages

# configuration
st.set_page_config(
    page_title="Compara",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

sftp_host = st.secrets["sftp"]["hostname"]
sftp_user = st.secrets["sftp"]["username"]
sftp_password = st.secrets["sftp"]["password"]
sftp_port = st.secrets["sftp"]["port"]


CHUNK_SIZE = 128 * 1024  # 64 KB optimizado para transferencias
MAX_RETRIES = 5
KEEPALIVE_INTERVAL = 25  # Segundos

# ESTILOS
# ESTILOS
with open('style/style.css') as f:
    css = f.read()
st.markdown(f'<style>{css}</style>', unsafe_allow_html=True)

menu_pages()


class SFTPComparator:
    def __init__(self, host, port, user, password):
        self.conn_params = {
            'host': host,
            'port': port,
            'user': user,
            'password': password
        }
        self.transport = None
        self.sftp = None
        self.CHUNK_SIZE = 128 * 1024  # 64 KB
        self.MAX_RETRIES = 3
        self.KEEPALIVE_INTERVAL = 35  # segundos

    def connect(self):
        """Establece conexión inicial"""
        self.transport = paramiko.Transport(
            (self.conn_params['host'], self.conn_params['port']))

        # Configurar keepalive solo si está disponible
        if hasattr(self.transport, 'set_keepalive'):
            self.transport.set_keepalive(self.KEEPALIVE_INTERVAL)

        self.transport.connect(
            username=self.conn_params['user'],
            password=self.conn_params['password']
        )
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def safe_keepalive(self):
        """Método seguro para enviar keepalives"""
        try:
            # Para versiones antiguas de Paramiko
            if hasattr(self.transport, 'send_keepalive'):
                self.transport.send_keepalive()
            else:
                # Método alternativo para mantener conexión activa
                self.sftp.listdir('.')
        except Exception as e:
            st.warning(f"Warning: Error enviando keepalive - {str(e)}")

    def reconnect(self):
        """Reconexión segura con parámetros guardados"""
        self.close()
        print("Reconectando...")
        self.connect()

    def close(self):
        """Cierre seguro de conexiones"""
        if self.sftp:
            self.sftp.close()
        if self.transport and self.transport.is_active():
            self.transport.close()

    def check_connection(self):
        """Verifica y mantiene la conexión activa"""
        if not self.transport or not self.transport.is_active():
            self.reconnect()
        elif self.sftp.get_channel().closed:
            self.reconnect()

    @staticmethod
    def safe_operation(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            retries = 0
            while retries < self.MAX_RETRIES:
                try:
                    self.check_connection()
                    return func(self, *args, **kwargs)
                except (socket.error, paramiko.SSHException, EOFError, AttributeError) as e:
                    print(f"Error: {e}. Reintento {
                          retries + 1}/{self.MAX_RETRIES}")
                    self.reconnect()
                    retries += 1
                    time.sleep(2 ** retries)
            raise ConnectionError("Máximo de reintentos alcanzado")
        return wrapper

    @safe_operation
    def count_rows(self, filepath):
        """Cuenta filas con manejo seguro de keepalive"""
        row_count = 0
        last_activity = time.time()

        with self.sftp.open(filepath, 'rb') as f:
            while True:
                chunk = f.read(self.CHUNK_SIZE)
                if not chunk:
                    break
                row_count += chunk.count(b'\n')

                # Mantener conexión activa de forma segura
                if time.time() - last_activity > self.KEEPALIVE_INTERVAL/2:
                    self.safe_keepalive()
                    last_activity = time.time()

        return row_count

    @safe_operation
    def get_file_stats(self, filepath):
        """Obtiene estadísticas del archivo"""
        return self.sftp.stat(filepath)

    def compare_files(self):
        """Función principal de comparación"""
        self.connect()
        yesterday_folder = (
            datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        results = []

        try:
            root_files = [f.filename for f in self.sftp.listdir_attr()
                          if f.filename.endswith('.csv') and not stat.S_ISDIR(f.st_mode)]

            progress_bar = st.progress(0)
            total_files = len(root_files)

            for index, filename in enumerate(root_files):
                progress = (index + 1) / total_files
                progress_bar.progress(
                    progress, f"{filename}    -    {index + 1}/{total_files}")
                try:
                    today_path = filename
                    yesterday_path = f"{yesterday_folder}/{filename}"

                    try:
                        self.get_file_stats(yesterday_path)
                    except FileNotFoundError:
                        continue

                    today_size = self.get_file_stats(today_path).st_size / 1024
                    yesterday_size = self.get_file_stats(
                        yesterday_path).st_size / 1024

                    # Contar filas
                    today_rows = self.count_rows(today_path)
                    yesterday_rows = self.count_rows(yesterday_path)

                    diferencias = today_size - yesterday_size
                    diferencias_rows = today_rows - yesterday_rows

                    results.append([
                        filename,
                        round(today_size, 2),
                        round(yesterday_size, 2),
                        round(diferencias, 2),
                        yesterday_rows,
                        today_rows,
                        diferencias_rows
                    ])
                    progress_bar.empty()
                except Exception as e:
                    st.write(f"Error procesando {filename}: {str(e)}")
                    continue

        finally:
            self.close()

        # Crear y mostrar DataFrame
        df = pd.DataFrame(results,
                          columns=["Archivo", "Tamaño Hoy (KB)", "Tamaño Ayer (KB)", "Variación (KB)", "Filas Ayer", "Filas hoy", "Variación Filas"])
        st.write("\nResultados de comparación:")
        # st.write(df.to_string(index=False))
        st.dataframe(df, height=650, width=1100)


# Uso
comparator = SFTPComparator(
    sftp_host,
    sftp_port,
    sftp_user,
    sftp_password
)

comparator.compare_files()
