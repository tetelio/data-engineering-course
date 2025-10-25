import requests
import boto3
import time
import json
import multiprocessing
import speedtest
from pathlib import Path
from dotenv import dotenv_values


# Load configuration values from the .env file
config = dotenv_values('.env')

# Create an s3 client object for uploading the files to the s3 bucket in the last step. Also, use the aws profile if provided
try:
    bucket_name = config['BUCKET_NAME']
    aws_profile = config['AWS_PROFILE']

    if aws_profile:
        session = boto3.Session(profile_name=aws_profile)
    else:
        session = boto3.Session()

    s3_client = session.client('s3')

except Exception as e:
    print(f"Failed to acquire a valid s3 client. Error:\n{e}")

# Ensure that the directories for our assets and encrypted assets exist
assets_path = Path('assets')
assets_path.mkdir(exist_ok=True)
encrypted_assets_path = Path("encrypted_assets")
encrypted_assets_path.mkdir(exist_ok=True)

# List of urls that contain the videos to encrypt
urls = [
    'https://github.com/tetelio/data-engineering-course/releases/download/v0.1.0/34406122.mp4',
    'https://github.com/tetelio/data-engineering-course/releases/download/v0.1.0/5159092.mp4',
    'https://github.com/tetelio/data-engineering-course/releases/download/v0.1.0/5157341.mp4',
    'https://github.com/tetelio/data-engineering-course/releases/download/v0.1.0/6139586.mp4',
    'https://github.com/tetelio/data-engineering-course/releases/download/v0.1.0/8928255.mp4',
    'https://github.com/tetelio/data-engineering-course/releases/download/v0.1.0/4769542.mp4',
    'https://github.com/tetelio/data-engineering-course/releases/download/v0.1.0/3255275.mp4',
    'https://github.com/tetelio/data-engineering-course/releases/download/v0.1.0/3196061.mp4',
    'https://github.com/tetelio/data-engineering-course/releases/download/v0.1.0/4778723.mp4'
]

# Read the key string from the .env file that will be used to encrypt the files
key_string = config['ENCRYPTION_KEY'].strip()
encryption_rounds = int(config['ENCRYPTION_ROUNDS'])
min_key_lenght = 10
if len(key_string) < min_key_lenght:
    print(f"The key length is too short, please, make it at least {min_key_lenght} characters")

# Transform the key string as a byte array in utf-8 (which converts the bytes into a numeric value between 0 and 255) and calculate its length in order to encrypt
key_bytes = bytearray(key_string.encode('utf-8'))
len_key_bytes = len(key_bytes)

# Set up our time tracking to monitor performance
start_time = time.time()
times = {} 

# Process each file sequentially
for i, url in enumerate(urls):
    times[i] = {}
    times[i]['download'] = {}
    times[i]['encrypt'] = {}
    times[i]['upload'] = {}

    print(f"\n----------Processing file {i}----------")
    # Download each file and write it to disk for persistence
    times[i]['download']['start'] = time.time() - start_time
    response = requests.get(url)

    format = url.split('.')[-1]
    stem = url.split('/')[-1].split('.')[-2]
    file_name = f"{stem}.{format}"
    file_path = assets_path / file_name

    with open(file_path, 'wb') as f:
        f.write(response.content)

    times[i]['download']['end'] = time.time() - start_time
    print(f"Downloaded contents of {url} into {file_path}")

    # Read the file from disk
    times[i]['encrypt']['start'] = time.time() - start_time
    response = requests.get(url)
    with open(file_path, 'rb') as f:
        file_bytes = bytearray(f.read())

    # Key has a fixed length key and files have a variety of byte-lengths, so key has to be repeated as many times as necessary to have the same length as the file bytes
    len_file_bytes = len(file_bytes)
    n_repeats = len_file_bytes // len_key_bytes
    remainder = len_file_bytes % len_key_bytes
    key_bytes_extended = key_bytes * n_repeats + key_bytes[:remainder]

    # Shift the byte values but not exceeding the value 255. If 255 is exceeded, value starts from 0 again. This happens for several rounds to fabricate a cpu-intensive task
    for r in range(encryption_rounds):
        for j in range(len(file_bytes)):
            # Add key + round number for extra variation
            file_bytes[j] = (file_bytes[j] + key_bytes_extended[j] + r) % 256

    encrypted_file_path = encrypted_assets_path / f"{file_path.stem}_encrypted{file_path.suffix}"

    # Write the resulting encrypted files to disk for persistence
    with open(encrypted_file_path, 'wb') as f:
        f.write(file_bytes)

    times[i]['encrypt']['end'] = time.time() - start_time
    print(f"Encrypted contents of {file_path} to {encrypted_file_path}")
    
    # Upload the encrypted file to the remote storage in the s3 bucket
    if s3_client:
        times[i]['upload']['start'] = time.time() - start_time
        file_path_str = str(encrypted_file_path)
        s3_client.upload_file(file_path_str, bucket_name, file_path_str)
        s3_file_path = f's3://{bucket_name}/{file_path_str}'

        times[i]['upload']['end'] = time.time() - start_time
        print(f"Uploaded contents of {url} to {s3_file_path}")

end_time = time.time()

# Write down the time data into a json file for posterior analysis
time_analysis_path = Path('time_analysis')
time_analysis_path.mkdir(exist_ok=True)
time_analysis_file_path = time_analysis_path / 'chapter-i.json'

with open(time_analysis_file_path, 'w') as f:
    f.write(json.dumps(times, indent=2))

total_time = end_time - start_time
print(f"\nTotal time is {int(total_time)}s")

# Check number of cores for performance tuning analysis
cpu_count = multiprocessing.cpu_count()

# Check internet speed test for performance tuning too
st = speedtest.Speedtest()

st.get_best_server()
download_speed = st.download()
upload_speed = st.upload()

download_mbytesps = download_speed / (1_000_000 * 8)
upload_mbytesps = upload_speed / (1_000_000 * 8)

performance_data = {
    'cpu_count': cpu_count,
    'download_MB/s': download_mbytesps,
    'upload_MB/s': upload_mbytesps
}

# Write down the time data into a json file for posterior analysis
time_analysis_performance_file_path = time_analysis_path / 'chapter-i-performance.json'

with open(time_analysis_performance_file_path, 'w') as f:
    f.write(json.dumps(performance_data, indent=2))

