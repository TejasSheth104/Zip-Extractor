import os
import requests
import tarfile
from bs4 import BeautifulSoup
from tqdm import tqdm

# Constants
BASE_URL = "https://dcapswoz.ict.usc.edu/wwwedaic/data/"
DATA_FOLDER = "data"
PROJECT_FOLDER = "Project"

# Create directories if not exist
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(PROJECT_FOLDER, exist_ok=True)

def get_tar_links():
    response = requests.get(BASE_URL)
    if response.status_code != 200:
        print("Failed to fetch the website")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    tar_links = [BASE_URL + a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.tar.gz')]
    return tar_links

def download_file(url, folder):
    filename = os.path.join(folder, url.split('/')[-1])
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        total_size = int(response.headers.get('content-length', 0))
        with open(filename, 'wb') as f, tqdm(
            desc=filename,
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024
        ) as bar:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
                bar.update(len(chunk))
    return filename

def extract_csv(tar_path, destination):
    with tarfile.open(tar_path, 'r:gz') as tar_ref:
        for member in tar_ref.getmembers():
            if "transcript" in member.name.lower() and member.name.endswith(".csv"):
                member.name = os.path.basename(member.name)
                tar_ref.extract(member, destination)
                print(f"Extracted {member.name} to {destination}")

def main():
    tar_links = get_tar_links()
    
    for link in tar_links:
        tar_path = download_file(link, DATA_FOLDER)
        extract_csv(tar_path, PROJECT_FOLDER)

    print("Task completed!")

if __name__ == "__main__":
    main()
