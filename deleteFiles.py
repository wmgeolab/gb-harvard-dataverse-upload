from prefect import flow
import subprocess
import requests

GbOpen = "/home/rohith/Desktop/DVHpc/geoBoundaries/database/geoBoundaries/releaseData/gbOpen/"
DVuploader="/sciclone/geounder/dev/geoBoundaries/scripts/geoBoundaryBot/external/DataVerse/DVUploader-v1.1.0.jar"
api_token = "2ed5ef8b-6f89-46bb-b140-a1c6914020ff"
dataset_id = 7242413 #7152306 #35614814

@flow(flow_run_name="gbOpenDV",log_prints=True)
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