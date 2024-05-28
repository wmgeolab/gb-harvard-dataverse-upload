import os
import shutil
import tempfile
import zipfile
import subprocess
import requests
import datetime
import glob
from prefect import flow

GbOpen = "/sciclone/geounder/dev/geoBoundaries/database/geoBoundaries/releaseData/gbOpen/"
DVuploader="/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/DataVerse/DVUploader-v1.1.0.jar"
chunkFiles="/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/DataVerse/chunkFiles/"
default_log_dir="/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/DataVerse/"
logFiles="/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/DataVerse/logFiles/"
api_token = "2ed5ef8b-6f89-46bb-b140-a1c6914020ff"
dataset_id = 7242413 #7152306 #35614814

def generate_flow_run_name():
    date = datetime.datetime.now(datetime.timezone.utc)

    return f"On-{date:%A}-{date:%B}-{date.day}-{date.year}"

@flow(name='Harvard DV: UploadFiles',flow_run_name=generate_flow_run_name,log_prints=True)
def uploadFilesDV():

    # Define the API endpoint URL to retrieve the list of files
    files_url = f"https://dataverse.harvard.edu/api/datasets/{dataset_id}"

    # Set the headers with the API token
    headers = {
        "Content-Type": "application/json",
        "X-Dataverse-key": api_token
    }

    # Send a GET request to retrieve the list of files
    response = requests.get(files_url, headers=headers)
    files_data = response.json()

    # Create temporary directory
    Gbtemp_dir = tempfile.mkdtemp()
    max_size = 1 * 1024 * 1024 * 1024# maximum size of each zip file in bytes
    chunk_num = 1  # initialize chunk number

    for directory in os.listdir(GbOpen):
        directory_path = os.path.join(GbOpen, directory)
        if os.path.isdir(directory_path):
            for subdir in ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]:
                subdir_path = os.path.join(directory_path, subdir)
                if os.path.isdir(subdir_path):
                    zip_file_path = None
                    for file in os.listdir(subdir_path):
                        if file.endswith('.zip'):
                            zip_file_path = os.path.join(subdir_path, file)
                            break
                    if zip_file_path is not None:
                        # Copy zip file to temporary directory
                        shutil.copy2(zip_file_path, Gbtemp_dir)
                        print(f"Copied {zip_file_path} to {Gbtemp_dir}")

    #Create the zip of temporary directory
    #zip_filename = '/home/rohith/Documents/temp_dir.zip'
    # Loop over all files in the temp directory
    for root, dirs, files in os.walk(Gbtemp_dir):
        # print(root,"root path")
        zip_size = 0  # initialize zip size for this chunk
        zip_name = f"Gbopen{chunk_num}.zip"  # initialize zip file name for this chunk
        zip_file_path = os.path.join(chunkFiles, zip_name)
        print(zip_file_path)
        zip_file = zipfile.ZipFile(zip_file_path, mode="w", compression=zipfile.ZIP_DEFLATED)

        # Loop over all files in this directory level
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)

            # If adding this file would exceed the max_size, close the current zip file and start a new one
            if zip_size + file_size > max_size:
                zip_file.close()
                zip_size = 0
                chunk_num += 1
                zip_name = f"Gbopen{chunk_num}.zip"
                zip_file_path = os.path.join(chunkFiles, zip_name)
                print(zip_file_path)
                zip_file = zipfile.ZipFile(zip_file_path, mode="w", compression=zipfile.ZIP_DEFLATED)

            # Add the file to the current zip file and update the zip size
            zip_file.write(file_path, arcname=file)
            zip_size += file_size

        # Close the current zip file
        zip_file.close()

    print(f"Created {chunk_num} zip files.")


def upload():
        # Execute the command in the terminal
    print("entered")
    for i in range(1, 11):
        filename = f"Gbopen{i}.zip"
        directory = f"/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/DataVerse/chunkFiles/{filename}"
        command = f"java -jar {DVuploader} -key=bcbc1d25-9435-479a-834f-d32801147906 -did=doi:10.7910/DVN/PGAIQY  -server=https://dataverse.harvard.edu {directory}"
        subprocess.run(command, shell=True)
        log_files = glob.glob(os.path.join(default_log_dir, 'DVUploaderLog__*.log'))
        if log_files:
            latest_log_file = max(log_files, key=os.path.getctime)
            # dest_log_path = os.path.join(logFiles, latest_log_file)
            
            # Move the log file to the log directory
            shutil.move(latest_log_file, logFiles)
            print(f"Moved {latest_log_file} to {logFiles}")
        else:
            print("No log files found.")


uploadFilesDV()
upload()
