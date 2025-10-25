from pathlib import Path
from dotenv import dotenv_values


# We load some configuration values from the .env file
config = dotenv_values('.env')

# We prepare directory variables and make sure that the directory that we will use for our decrypted assets exists
assets_path = Path('assets')
encrypted_assets_path = Path("encrypted_assets")
decrypted_assets_path = Path('decrypted_assets')
decrypted_assets_path.mkdir(exist_ok=True)

# We read the key string from the .env file that we will use to encrypt the files
key_string = config['ENCRYPTION_KEY'].strip()
encryption_rounds = int(config['ENCRYPTION_ROUNDS'])
min_key_lenght = 10
if len(key_string) < min_key_lenght:
    print(f"The key length is too short, please, make it at least {min_key_lenght} characters")

# We will need the key string as a byte array in utf-8 (which converts the bytes into a numeric value between 0 and 255) and its length in order to encrypt
key_bytes = bytearray(key_string.encode('utf-8'))
len_key_bytes = len(key_bytes)

for file_path in encrypted_assets_path.iterdir():
    with open(file_path, 'rb') as f:
        file_bytes = bytearray(f.read())

    # We have a fixed length key and our files have a variety of byte-lengths, so we repeat the key as many times as we need to have the same length as the file bytes
    len_file_bytes = len(file_bytes)
    n_repeats = len_file_bytes // len_key_bytes
    remainder = len_file_bytes % len_key_bytes
    key_bytes_extended = key_bytes * n_repeats + key_bytes[:remainder]

    # We shift the byte values but we never recede 0. If we recede 0, we start counting 255 backwards. We do it for several rounds to correspond with the encryption process
    for r in range(encryption_rounds):
        for i in range(len(file_bytes)):
            # Add key + round number for extra variation
            file_bytes[i] = (file_bytes[i] - key_bytes_extended[i] - r) % 256

    # We read the bytes of the original file 
    original_file_path = assets_path / str(file_path.name).replace('_encrypted', '')

    with open(original_file_path, 'rb') as f:
        original_file_bytes = bytearray(f.read())

    # We compare the original bytes with the decrypted bytes. If they are not the same, something went wrong so we dont save that file to disk
    if file_bytes != original_file_bytes:
        print(f"Decryption failed for {file_path}. The file bytes are not the same as the original ones. Did you change the encryption key value in the .env file?")
        # continue jumps into the next iteration without running more code
        continue

    # We write the decrypted file bytes to disk
    decrypted_file_path = decrypted_assets_path / f"{file_path.stem}_decrypted{file_path.suffix}"

    with open(decrypted_file_path, 'wb') as f:
        f.write(file_bytes)

    print(f"Decrypted contents of {file_path} to {decrypted_file_path}")

