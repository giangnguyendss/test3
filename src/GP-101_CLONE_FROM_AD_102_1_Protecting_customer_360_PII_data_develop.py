%pip install cryptography

spark.catalog.setCurrentCatalog("purgo_databricks")

# ----------------------------------------------------------------------------------------
# Databricks PySpark Script: Encrypt PII Columns in customer_360_raw_clone and Save Key
# ----------------------------------------------------------------------------------------
# Purpose:
#   - Drop and recreate purgo_playground.customer_360_raw_clone as a replica of purgo_playground.customer_360_raw
#   - Encrypt PII columns (name, email, phone, zip) in the clone table using AES-256-GCM
#   - Save the encryption key as a JSON file named encryption_key_<current_datetime>.json in the specified volume
#   - Ensure all error handling, data validation, and compliance with Databricks best practices
#   - All code is idempotent and repeatable
# ----------------------------------------------------------------------------------------

# ---------------------------------
# Imports and Configuration Section
# ---------------------------------
from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType  
from datetime import datetime  
import base64  
import secrets  
import json  
from cryptography.hazmat.primitives.ciphers.aead import AESGCM  
import os  

# -------------------------------
# Setup: Table and Path Constants
# -------------------------------
CATALOG = "purgo_databricks"
SCHEMA = "purgo_playground"
SRC_TABLE = f"{CATALOG}.{SCHEMA}.customer_360_raw"
CLONE_TABLE = f"{CATALOG}.{SCHEMA}.customer_360_raw_clone"
PII_COLS = ["name", "email", "phone", "zip"]
KEY_DIR = "/Volumes/agilisium_playground/purgo_playground/de_dq"
KEY_FILE_PREFIX = "encryption_key_"
KEY_FILE_SUFFIX = ".json"
KEY_LENGTH_BYTES = 32  # 256 bits for AES-256
AES_GCM_NONCE_LENGTH = 12  # 96 bits, recommended for GCM

# -------------------------------
# Helper Functions
# -------------------------------

def get_current_dt_str():
    # Returns current datetime as yyyyMMdd_HHmmss
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def generate_aes256_key():
    # Generate a 32-byte (256-bit) random key and return (bytes, base64-encoded string)
    try:
        key_bytes = secrets.token_bytes(KEY_LENGTH_BYTES)
        key_b64 = base64.b64encode(key_bytes).decode("utf-8")
        if len(key_bytes) != 32 or len(key_b64) != 44:
            raise Exception("Key length invalid")
        return key_bytes, key_b64
    except Exception as e:
        raise RuntimeError("Encryption key generation failed.") from e

def save_key_json(key_b64, key_dir, dt_str):
    # Save the key as JSON file in the specified directory
    key_file = f"{key_dir}/{KEY_FILE_PREFIX}{dt_str}{KEY_FILE_SUFFIX}"
    key_json = {"encryption_key": key_b64}
    try:
        with open(key_file, "w") as f:
            json.dump(key_json, f)
        # Validate file exists and is readable
        with open(key_file, "r") as f:
            loaded = json.load(f)
        if loaded.get("encryption_key") != key_b64 or set(loaded.keys()) != {"encryption_key"} or len(loaded["encryption_key"]) != 44:
            raise Exception("Key file content invalid")
    except Exception as e:
        raise RuntimeError(f"Unable to write encryption key file to {key_dir}.") from e
    return key_file

def aes_gcm_encrypt(plaintext, key_bytes):
    # Encrypt plaintext (str) using AES-256-GCM, return base64-encoded (nonce + ciphertext)
    if plaintext is None:
        return None
    if not isinstance(plaintext, str):
        plaintext = str(plaintext)
    try:
        aesgcm = AESGCM(key_bytes)
        nonce = secrets.token_bytes(AES_GCM_NONCE_LENGTH)
        ct = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
        encrypted = base64.b64encode(nonce + ct).decode("utf-8")
        return encrypted
    except Exception as e:
        raise RuntimeError("Encryption failed for value.") from e

def get_user():
    # Get current user for logging
    try:
        return spark.sql("SELECT current_user() as user").collect()[0]["user"]
    except Exception:
        return "unknown"

# -------------------------------
# Step 1: Drop and Recreate Clone Table
# -------------------------------
try:
    spark.sql(f"DROP TABLE IF EXISTS {CLONE_TABLE}")
except Exception as e:
    # If table does not exist, ignore
    pass

# Validate source table exists
try:
    spark.table(SRC_TABLE)
except Exception as e:
    raise RuntimeError(f"Source table {SRC_TABLE} does not exist.") from e

# Create clone as replica of source
spark.sql(f"CREATE TABLE {CLONE_TABLE} AS SELECT * FROM {SRC_TABLE}")

# -------------------------------
# Step 2: Generate and Save Encryption Key
# -------------------------------
dt_str = get_current_dt_str()
key_bytes, key_b64 = generate_aes256_key()
key_file = save_key_json(key_b64, KEY_DIR, dt_str)

# -------------------------------
# Step 3: Encrypt PII Columns in Clone Table
# -------------------------------
# Read clone table
df = spark.table(CLONE_TABLE)

# Define UDF for encryption
def encrypt_udf(val):
    try:
        return aes_gcm_encrypt(val, key_bytes)
    except Exception as e:
        raise RuntimeError("Encryption failed for column value.") from e

encrypt_udf_pyspark = F.udf(encrypt_udf, StringType())

# Apply encryption to PII columns
encrypted_df = df
for col in PII_COLS:
    encrypted_df = encrypted_df.withColumn(col, encrypt_udf_pyspark(F.col(col)))

# Validate schema and column count
if encrypted_df.schema != df.schema or len(encrypted_df.columns) != len(df.columns):
    raise RuntimeError("Schema or column count mismatch after encryption.")

# Overwrite clone table with encrypted data
encrypted_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(CLONE_TABLE)

# -------------------------------
# Step 4: Data Quality and Logging (Optional)
# -------------------------------
# Validate encrypted columns are base64 and not plaintext, nulls remain null
for col in PII_COLS:
    vals = encrypted_df.select(col).rdd.flatMap(lambda x: x).collect()
    for v in vals:
        if v is None:
            continue
        if not isinstance(v, str) or len(v) < 24 or not all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=" for c in v):
            raise RuntimeError(f"Encrypted value in column {col} is not valid base64.")

# Optionally log to pii_encryption_log (uncomment if required)
# log_df = spark.createDataFrame([{
#     "process_name": "customer_360_pii_encryption",
#     "source_table": SRC_TABLE,
#     "target_table": CLONE_TABLE,
#     "encrypted_columns": ",".join(PII_COLS),
#     "key_file": key_file,
#     "status": "SUCCESS",
#     "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#     "user": get_user()
# }])
# log_df.write.mode("append").insertInto(f"{CATALOG}.{SCHEMA}.pii_encryption_log")

# -------------------------------
# End of Script
# -------------------------------
# Encryption of PII columns in customer_360_raw_clone complete. Key saved as JSON.
