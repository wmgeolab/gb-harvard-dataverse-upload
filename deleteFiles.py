from prefect import flow
import subprocess
import requests
import datetime
from prefect import flow

api_token = "bcbc1d25-9435-479a-834f-d32801147906"
dataset_id = 3661110 #7242413

def generate_flow_run_name():
    date = datetime.datetime.now(datetime.timezone.utc)

    return f"On-{date:%A}-{date:%B}-{date.day}-{date.year}"

@flow(name='Harvard DV: DeleteFiles',flow_run_name=generate_flow_run_name,log_prints=True)
def deleteFilesDV():

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
    print(files_data)

    # Check if "files" key contains data
    if "files" in files_data["data"]["latestVersion"]:

        # Iterate over the files and delete them individually
        for file_data in files_data["data"]["latestVersion"]["files"]:
            datafile_id = file_data['dataFile']['id']
            delete_url = f"https://dataverse.harvard.edu/dvn/api/data-deposit/v1.1/swordv2/edit-media/file/{datafile_id}"
            print(delete_url)

            command = ["curl", "-u", f"{api_token}:", "-X", "DELETE", delete_url]

            try:
                subprocess.run(command, check=True)
                print("File deletion successful!")
            except subprocess.CalledProcessError as e:
                print(f"File deletion failed. Error: {e}")

    else:
        print("No files found.")

deleteFilesDV()
