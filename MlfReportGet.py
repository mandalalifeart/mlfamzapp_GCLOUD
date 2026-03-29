import requests

def download_encrypted_file(file_url):
	response = requests.get(file_url)
	print("response.status_code"+str(response.status_code))
	if response.status_code == 200:
		return response.content  # Returns the content of the file
	else:
		print("Failed to download the file.")
		return None

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import base64

def decrypt_file(encrypted_data, key, iv):
    key_bytes = base64.b64decode(key)
    iv_bytes = base64.b64decode(iv)
    cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(iv_bytes), backend=default_backend())
    decryptor = cipher.decryptor()
    decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
    return decrypted_data

import io
import csv

def process_csv_data(decrypted_data):
    file_like = io.StringIO(decrypted_data.decode('utf-8'))
    reader = csv.DictReader(file_like)
    for row in reader:
        print(row)  # Handle each row as needed
		


def handle_report_document(document_details):
    # Download the encrypted report
    encrypted_file = download_encrypted_file(document_details['url'])
    if encrypted_file:
        # Decrypt the downloaded file
        decrypted_data = decrypt_file(encrypted_file, document_details['encryptionDetails']['key'], document_details['encryptionDetails']['initializationVector'])
        if decrypted_data:
            # Process the decrypted data
            process_csv_data(decrypted_data)
        else:
            print("Decryption failed.")
    else:
        print("Download failed.")


