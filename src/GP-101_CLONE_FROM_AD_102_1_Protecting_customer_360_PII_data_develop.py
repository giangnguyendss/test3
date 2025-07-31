%pip install cryptography

spark.catalog.setCurrentCatalog("purgo_databricks")

# ----------------------------------------------------------------------------------------
# Databricks PySpark Script: Encrypt PII columns in purgo_playground.customer_360_raw_clone
# ----------------------------------------------------------------------------------------
# This script:
#   - Drops and recreates purgo_playground.customer_360_raw_clone as a replica of purgo_playground.customer_360_raw
#   - Encrypts columns: name, email, phone, zip using AES-256 (CBC, PKCS7 padding)
#   - Saves the encryption key as a JSON file in /Volumes/agilisium_playground/purgo_playground/de_dq
#   - Logs process status to purgo_playground.pii_encryption_log
#   - Handles errors and edge cases as per requirements
# ----------------------------------------------------------------------------------------

# ----------------------------------------
# Imports
# ----------------------------------------
import base64  
import json    
import os      
from datetime import datetime, timezone  
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, StructType, StructField, LongType, DateType  
from pyspark.sql.utils import AnalysisException  
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes  
from cryptography.hazmat.backends import default_backend  
from cryptography.hazmat.primitives import padding  

# ----------------------------------------
# Configuration and Constants
# ----------------------------------------
CATALOG = "purgo_databricks"
SCHEMA = "purgo_playground"
SRC_TABLE = f"{CATALOG}.{SCHEMA}.customer_360_raw"
CLONE_TABLE = f"{CATALOG}.{SCHEMA}.customer_360_raw_clone"
LOG_TABLE = f"{CATALOG}.{SCHEMA}.pii_encryption_log"
VOLUME_PATH = "/Volumes/agilisium_playground/purgo_playground/de_dq"
PII_COLS = ["name", "email", "phone", "zip"]
ENCRYPTION_ALGO = "AES-256"
PROCESS_NAME = "pii_encryption"
USER = os.environ.get("USER", "databricks_user")  # fallback for Databricks

# ----------------------------------------
# Helper Functions for AES-256 Encryption/Decryption
# ----------------------------------------

def generate_aes256_key():
    # Generate a 32-byte (256-bit) random key
    return os.urandom(32)

def base64_encode_key(key_bytes):
    # Return base64-encoded string
    return base64.b64encode(key_bytes).decode("utf-8")

def get_current_utc_iso():
    # Return current UTC time in ISO 8601 format
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_key_json_path():
    # Compose key file path with UTC timestamp
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{VOLUME_PATH}/encryption_key_{ts}.json"

def pad_pkcs7(data):
    # Pad data to 16 bytes (AES block size)
    padder = padding.PKCS7(128).padder()
    padded = padder.update(data.encode("utf-8")) + padder.finalize()
    return padded

def aes256_encrypt(plaintext, key):
    # Encrypt a string using AES-256-CBC with random IV, return base64(iv + ciphertext)
    if plaintext is None:
        return None
    if not isinstance(plaintext, str):
        raise ValueError("Encryption failed for column: value is not string")
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    padded = pad_pkcs7(plaintext)
    ct = encryptor.update(padded) + encryptor.finalize()
    return base64.b64encode(iv + ct).decode("utf-8")

def save_key_json(key_bytes, algo, path):
    # Save key as JSON file with metadata
    if algo != "AES-256":
        raise ValueError(f"Unsupported encryption algorithm: {algo}")
    key_b64 = base64_encode_key(key_bytes)
    key_json = {
        "key": key_b64,
        "algorithm": algo,
        "created_at": get_current_utc_iso()
    }
    try:
        with open(path, "w") as f:
            json.dump(key_json, f)
    except Exception as e:
        raise IOError(f"Unable to write encryption key file to {VOLUME_PATH}: {str(e)}")
    return key_json

def log_process(status, key_file, error_message=None):
    # Log process to pii_encryption_log table
    now = get_current_utc_iso()
    encrypted_columns = ",".join(PII_COLS)
    log_row = [(PROCESS_NAME, f"{SCHEMA}.customer_360_raw", f"{SCHEMA}.customer_360_raw_clone", encrypted_columns, key_file, status, now, USER)]
    schema = StructType([
        StructField("process_name", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("encrypted_columns", StringType(), True),
        StructField("key_file", StringType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user", StringType(), True),
    ])
    log_df = spark.createDataFrame(log_row, schema=schema)
    log_df.write.format("delta").mode("append").saveAsTable(LOG_TABLE)
    if status == "failed" and error_message:
        print(f"Process failed: {error_message}")

# ----------------------------------------
# Main Encryption Process
# ----------------------------------------
def main():
    key_file_path = None
    try:
        # Step 1: Drop clone table if exists
        try:
            spark.sql(f"DROP TABLE IF EXISTS {CLONE_TABLE}")
        except Exception as e:
            # Log and continue (table may not exist)
            pass

        # Step 2: Create clone as replica of source
        try:
            spark.sql(f"CREATE TABLE {CLONE_TABLE} AS SELECT * FROM {SRC_TABLE}")
        except AnalysisException as e:
            # Source table missing
            log_process("failed", "", f"Source table {SCHEMA}.customer_360_raw does not exist")
            raise RuntimeError(f"Source table {SCHEMA}.customer_360_raw does not exist")
        except Exception as e:
            log_process("failed", "", f"Failed to create clone table: {str(e)}")
            raise

        # Step 3: Generate encryption key and save as JSON
        key = generate_aes256_key()
        key_file_path = get_key_json_path()
        try:
            save_key_json(key, ENCRYPTION_ALGO, key_file_path)
        except Exception as e:
            log_process("failed", key_file_path, str(e))
            raise

        # Step 4: Read clone table and encrypt PII columns
        df = spark.table(CLONE_TABLE)
        # Define UDF for encryption
        def encrypt_udf(val):
            try:
                return aes256_encrypt(val, key)
            except Exception as e:
                raise ValueError(f"Encryption failed for column: {str(e)}")
        encrypt_udf_spark = F.udf(encrypt_udf, StringType())
        encrypted_df = df
        for col in PII_COLS:
            encrypted_df = encrypted_df.withColumn(col, encrypt_udf_spark(F.col(col)))
        # Ensure schema matches before write
        if encrypted_df.schema != df.schema:
            log_process("failed", key_file_path, "Schema mismatch after encryption")
            raise RuntimeError("Schema mismatch after encryption")
        # Step 5: Overwrite clone table with encrypted data
        encrypted_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(CLONE_TABLE)

        # Step 6: Validate key file exists
        if not os.path.exists(key_file_path):
            log_process("failed", key_file_path, f"Encryption key file not found: {key_file_path}")
            raise RuntimeError(f"Encryption key file not found: {key_file_path}")

        # Step 7: Log success
        log_process("success", key_file_path)
        print(f"Encryption completed successfully. Key file: {key_file_path}")

    except Exception as e:
        # Log failure if not already logged
        if key_file_path:
            log_process("failed", key_file_path, str(e))
        else:
            log_process("failed", "", str(e))
        raise

# ----------------------------------------
# Execute Main Process
# ----------------------------------------
if __name__ == "__main__":
    main()
# ----------------------------------------------------------------------------------------
# End of script
# ----------------------------------------------------------------------------------------
