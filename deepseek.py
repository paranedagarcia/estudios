import streamlit as st
import paramiko
from datetime import datetime, timedelta
import os
from io import StringIO


def get_yesterday_folder():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_sftp_connection():
    try:
        hostname = st.secrets["sftp"]["hostname"]
        username = st.secrets["sftp"]["username"]
        password = st.secrets["sftp"]["password"]
        port = st.secrets["sftp"].get("port", 22)

        transport = paramiko.Transport((hostname, port))
        transport.connect(username=username, password=password)
        return paramiko.SFTPClient.from_transport(transport)
    except Exception as e:
        st.error(f"Error de conexión SFTP: {str(e)}")
        return None


def get_file_stats(sftp, file_path):
    try:
        file_size = sftp.stat(file_path).st_size / 1024  # Tamaño en KB
        return file_size
    except:
        return None


def count_rows_large_file(sftp, file_path):
    try:
        with sftp.file(file_path, 'r') as remote_file:
            row_count = 0
            chunk_size = 1024 * 1024  # 1 MB por chunk
            buffer = ''

            while True:
                data = remote_file.read(chunk_size).decode()
                if not data:
                    break

                buffer += data
                lines = buffer.split('\n')

                # Mantener el último fragmento incompleto
                buffer = lines.pop(-1) if len(lines) else ''

                row_count += len(lines)

            row_count += 1 if buffer else 0  # Última línea sin salto
            return row_count
    except Exception as e:
        st.error(f"Error leyendo {file_path}: {str(e)}")
        return None


def main():
    st.title("Comparador de Archivos CSV")

    with st.spinner("Conectando al SFTP..."):
        sftp = get_sftp_connection()

    if not sftp:
        return

    yesterday_folder = get_yesterday_folder()
    comparison_data = []

    try:
        with st.spinner("Buscando archivos..."):
            today_files = [
                f for f in sftp.listdir() if f.lower().endswith('.csv')]
            yesterday_files_path = f"./{yesterday_folder}/"

            try:
                yesterday_files = sftp.listdir(yesterday_files_path)
            except FileNotFoundError:
                st.error(
                    f"No se encontró la carpeta de ayer ({yesterday_folder})")
                return

        progress_bar = st.progress(0)
        total_files = len(today_files)

        for index, filename in enumerate(today_files):
            progress = (index + 1) / total_files
            progress_bar.progress(progress)

            today_path = filename
            yesterday_path = os.path.join(yesterday_files_path, filename)

            # Obtener estadísticas
            today_size = get_file_stats(sftp, today_path)
            yesterday_size = get_file_stats(
                sftp, yesterday_path) if filename in yesterday_files else None

            # Contar filas
            today_rows = count_rows_large_file(
                sftp, today_path) if today_size else None
            yesterday_rows = count_rows_large_file(
                sftp, yesterday_path) if yesterday_size else None

            comparison_data.append({
                'Archivo': filename,
                'Tamaño Hoy (KB)': round(today_size, 2) if today_size else 'N/A',
                'Tamaño Ayer (KB)': round(yesterday_size, 2) if yesterday_size else 'N/A',
                'Filas Hoy': today_rows or 'N/A',
                'Filas Ayer': yesterday_rows or 'N/A'
            })

        progress_bar.empty()

        # Mostrar resultados
        st.dataframe(
            data=comparison_data,
            column_config={
                "Archivo": "Archivo",
                "Tamaño Hoy (KB)": st.column_config.NumberColumn(format="%.2f KB"),
                "Tamaño Ayer (KB)": st.column_config.NumberColumn(format="%.2f KB"),
                "Filas Hoy": "Filas Hoy",
                "Filas Ayer": "Filas Ayer"
            },
            hide_index=True,
            use_container_width=True
        )

    finally:
        sftp.close()


main()

if __name__ == "__main__":
    main()
