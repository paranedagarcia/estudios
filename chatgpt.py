import streamlit as st
import paramiko
import pandas as pd
import os
from datetime import datetime, timedelta
import toml

# Load secrets
secrets = toml.load(".streamlit/secrets.toml")
sftp_host = secrets['sftp']['hostname']
sftp_port = secrets['sftp']['port']
sftp_username = secrets['sftp']['username']
sftp_password = secrets['sftp']['password']

# Define SFTP connection function


def connect_sftp():
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_username, password=sftp_password)
    return paramiko.SFTPClient.from_transport(transport)

# Define function to get file details from SFTP root or specific folder


def get_csv_details(sftp, folder="."):
    csv_details = []
    for file_attr in sftp.listdir_attr(folder):
        if file_attr.filename.endswith(".csv") and folder == ".":
            file_path = file_attr.filename
        elif file_attr.filename.endswith(".csv") and folder != ".":
            file_path = f"{folder}/{file_attr.filename}"
        else:
            continue

        file_size_kb = file_attr.st_size / 1024

        with sftp.file(file_path, "r") as file:
            row_count = sum(1 for line in file) - 1  # Exclude header

        csv_details.append({
            "file_name": file_attr.filename,
            "size_kb": round(file_size_kb, 2),
            "row_count": row_count,
        })
    st.write(f"Found {len(csv_details)} CSV files in folder '{folder}'.")
    st.write(csv_details)
    return csv_details

# Main Streamlit app


def main():
    st.title("SFTP CSV Comparison")

    # Calculate yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        with connect_sftp() as sftp:
            st.write("Connected to SFTP server.")

            # Get details from root folder
            st.write("Fetching details from root folder...")
            root_details = get_csv_details(sftp, folder=".")

            # Get details from yesterday's folder
            st.write(f"Fetching details from folder {yesterday}...")
            try:
                yesterday_details = get_csv_details(sftp, folder=yesterday)
            except FileNotFoundError:
                st.error(
                    f"Folder '{yesterday}' does not exist on the SFTP server.")
                return

            # Merge and compare data
            root_df = pd.DataFrame(root_details)
            yesterday_df = pd.DataFrame(yesterday_details)

            comparison_df = pd.merge(
                root_df, yesterday_df, on="file_name", suffixes=("_today", "_yesterday")
            )

            st.write("Comparison Results:")
            st.dataframe(comparison_df)

    except Exception as e:
        st.error(f"An error occurred: {e}")


main()

if __name__ == "__main__":
    main()
