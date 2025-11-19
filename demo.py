import os
import tempfile
import requests
def download_to_temp(url):
    """
    Download a file from URL into a temporary folder.
    Returns the full path of the downloaded file.
    """
    # Create a temp directory
    temp_dir = tempfile.mkdtemp()
    # Extract filename from URL
    filename = url.split("/")[-1]
    file_path = os.path.join(temp_dir, filename)
    # Download file
    response = requests.get(url, stream=True)
    response.raise_for_status()   # Raise error if download fails
    # Save file
    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return file_path

download_to_temp("https://attachments.happay.in/attachment/v1/get-file-from-temporary-link/404b8dd0-c52c-11f0-8f51-0242ac110004HP.pdf")