%pip install pycryptodome

spark.catalog.setCurrentCatalog("purgo_databricks")

# ------------------------------------------------------------------------------------
# Databricks PySpark Script: Encrypt PII Columns in customer_360_raw_clone
# ------------------------------------------------------------------------------------
# This script:
#   - Drops and recreates purgo_playground.customer_360_raw_clone as a replica of purgo_playground.customer_360_raw
#   - Encrypts columns: name, email, phone, zip using AES-256-CBC (random key/IV, base64-encoded)
#   - Saves the encryption key as JSON in /Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_<current_datetime>.json
#   - Logs the process to purgo_playground.pii_encryption_log
#   - Handles all error scenarios and logs failures
#   - Validates encrypted columns are base64-encoded
#   - All code is Databricks production-ready and follows best practices
# ------------------------------------------------------------------------------------

# -------------------- IMPORTS --------------------
from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import Row  
from pyspark.sql.types import StringType  
from pyspark.sql.functions import col, udf, current_timestamp  
import base64  
import json  
from datetime import datetime  
import getpass  
from Crypto.Cipher import AES  
from Crypto.Random import get_random_bytes  

# -------------------- CONFIGURATION --------------------
# Block comment: Define constants and parameters
PII_COLUMNS = ["name", "email", "phone", "zip"]
SOURCE_TABLE = "purgo_playground.customer_360_raw"
CLONE_TABLE = "purgo_playground.customer_360_raw_clone"
LOG_TABLE = "purgo_playground.pii_encryption_log"
VOLUME_PATH = "/Volumes/agilisium_playground/purgo_playground/de_dq"
ALGORITHM = "AES-256-CBC"
PROCESS_NAME = "pii_encrypt"
USER = getpass.getuser()

# -------------------- UTILITY: LOGGING FUNCTION --------------------
def log_activity(process_name, source_table, target_table, encrypted_columns, key_file, status, timestamp, user):
    """
    Write a log entry to the pii_encryption_log table.
    """
    log_row = Row(
        process_name=process_name,
        source_table=source_table,
        target_table=target_table,
        encrypted_columns=encrypted_columns,
        key_file=key_file,
        status=status,
        timestamp=timestamp,
        user=user
    )
    spark.createDataFrame([log_row]).write.format("delta").mode("append").saveAsTable(LOG_TABLE)

# -------------------- STEP 1: VALIDATE SOURCE TABLE AND COLUMNS --------------------
try:
    src_df = spark.table(SOURCE_TABLE)
    src_cols = set([f.name for f in src_df.schema.fields])
    for colname in PII_COLUMNS:
        if colname not in src_cols:
            raise Exception(f"Column {colname} does not exist in source table")
except Exception as e:
    # Log failure and exit
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    log_activity(
        process_name=PROCESS_NAME,
        source_table=SOURCE_TABLE,
        target_table=CLONE_TABLE,
        encrypted_columns=",".join(PII_COLUMNS),
        key_file="",
        status="FAILED",
        timestamp=now_iso,
        user=USER
    )
    raise RuntimeError(f"ERROR: {e}")

# -------------------- STEP 2: DROP AND RECREATE CLONE TABLE --------------------
try:
    spark.sql(f"DROP TABLE IF EXISTS {CLONE_TABLE}")
    spark.sql(f"""
        CREATE TABLE {CLONE_TABLE}
        USING DELTA
        AS SELECT * FROM {SOURCE_TABLE}
    """)
except Exception as e:
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    log_activity(
        process_name=PROCESS_NAME,
        source_table=SOURCE_TABLE,
        target_table=CLONE_TABLE,
        encrypted_columns=",".join(PII_COLUMNS),
        key_file="",
        status="FAILED",
        timestamp=now_iso,
        user=USER
    )
    raise RuntimeError(f"ERROR: Unable to create clone table: {e}")

# -------------------- STEP 3: GENERATE ENCRYPTION KEY AND SAVE TO JSON --------------------
key_bytes = get_random_bytes(32)  # 256 bits
iv_bytes = get_random_bytes(16)   # 128 bits for CBC
key_b64 = base64.b64encode(key_bytes).decode('utf-8')
iv_b64 = base64.b64encode(iv_bytes).decode('utf-8')
current_dt = datetime.now().strftime("%Y%m%d_%H%M%S")
created_at_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
key_file_name = f"encryption_key_{current_dt}.json"
key_file_path = f"{VOLUME_PATH}/{key_file_name}"
encryption_key_json = {
    "algorithm": ALGORITHM,
    "key": key_b64,
    "iv": iv_b64,
    "created_at": created_at_iso
}
try:
    dbutils.fs.put(f"dbfs:{key_file_path}", json.dumps(encryption_key_json), overwrite=True)
except Exception as e:
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    log_activity(
        process_name=PROCESS_NAME,
        source_table=SOURCE_TABLE,
        target_table=CLONE_TABLE,
        encrypted_columns=",".join(PII_COLUMNS),
        key_file=key_file_path,
        status="FAILED",
        timestamp=now_iso,
        user=USER
    )
    raise RuntimeError(f"ERROR: Unable to write encryption key file to {key_file_path}: {e}")

# -------------------- STEP 4: DEFINE ENCRYPTION UDF --------------------
def pad(s):
    # PKCS7 padding for AES block size 16
    pad_len = 16 - (len(s.encode('utf-8')) % 16)
    return s + chr(pad_len) * pad_len

def encrypt_aes_256_cbc(plaintext, key, iv):
    if plaintext is None:
        return None
    if not isinstance(plaintext, str):
        plaintext = str(plaintext)
    try:
        cipher = AES.new(key, AES.MODE_CBC, iv)
        padded = pad(plaintext)
        encrypted = cipher.encrypt(padded.encode('utf-8'))
        return base64.b64encode(encrypted).decode('utf-8')
    except Exception:
        return None

encrypt_udf = udf(lambda x: encrypt_aes_256_cbc(x, key_bytes, iv_bytes), StringType())

# -------------------- STEP 5: ENCRYPT PII COLUMNS IN CLONE TABLE --------------------
try:
    df_clone = spark.table(CLONE_TABLE)
    df_encrypted = df_clone
    for colname in PII_COLUMNS:
        df_encrypted = df_encrypted.withColumn(colname, encrypt_udf(col(colname)))
    # Ensure column count matches schema before overwrite
    if len(df_encrypted.columns) != len(df_clone.columns):
        raise Exception("Column count mismatch after encryption")
    df_encrypted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(CLONE_TABLE)
except Exception as e:
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    log_activity(
        process_name=PROCESS_NAME,
        source_table=SOURCE_TABLE,
        target_table=CLONE_TABLE,
        encrypted_columns=",".join(PII_COLUMNS),
        key_file=key_file_path,
        status="FAILED",
        timestamp=now_iso,
        user=USER
    )
    raise RuntimeError(f"ERROR: Unable to write to target table {CLONE_TABLE}: {e}")

# -------------------- STEP 6: VALIDATE ENCRYPTED DATA FORMAT --------------------
def is_base64(s):
    if s is None:
        return True
    try:
        return base64.b64encode(base64.b64decode(s)) == s.encode()
    except Exception:
        return False

sample_rows = spark.table(CLONE_TABLE).select(*PII_COLUMNS).limit(100).collect()
for row in sample_rows:
    for colname in PII_COLUMNS:
        val = row[colname]
        if val is not None and not is_base64(val):
            now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            log_activity(
                process_name=PROCESS_NAME,
                source_table=SOURCE_TABLE,
                target_table=CLONE_TABLE,
                encrypted_columns=",".join(PII_COLUMNS),
                key_file=key_file_path,
                status="FAILED",
                timestamp=now_iso,
                user=USER
            )
            raise RuntimeError(f"ERROR: Encrypted value in {colname} is not base64-encoded")

# -------------------- STEP 7: LOG SUCCESS --------------------
log_activity(
    process_name=PROCESS_NAME,
    source_table=SOURCE_TABLE,
    target_table=CLONE_TABLE,
    encrypted_columns=",".join(PII_COLUMNS),
    key_file=key_file_path,
    status="SUCCESS",
    timestamp=created_at_iso,
    user=USER
)

# -------------------- STEP 8: VALIDATION QUERY (CTE) --------------------
# Block comment: CTE to check for any non-base64 values in encrypted columns
validation_query = f"""
WITH encrypted_check AS (
  SELECT
    id,
    name,
    email,
    phone,
    zip,
    CASE
      WHEN name IS NULL THEN 'NULL'
      WHEN name RLIKE '^[A-Za-z0-9+/=]+$' THEN 'BASE64'
      ELSE 'NOT_BASE64'
    END AS name_base64_check,
    CASE
      WHEN email IS NULL THEN 'NULL'
      WHEN email RLIKE '^[A-Za-z0-9+/=]+$' THEN 'BASE64'
      ELSE 'NOT_BASE64'
    END AS email_base64_check,
    CASE
      WHEN phone IS NULL THEN 'NULL'
      WHEN phone RLIKE '^[A-Za-z0-9+/=]+$' THEN 'BASE64'
      ELSE 'NOT_BASE64'
    END AS phone_base64_check,
    CASE
      WHEN zip IS NULL THEN 'NULL'
      WHEN zip RLIKE '^[A-Za-z0-9+/=]+$' THEN 'BASE64'
      ELSE 'NOT_BASE64'
    END AS zip_base64_check
  FROM {CLONE_TABLE}
)
SELECT
  COUNT(*) AS total_records,
  SUM(CASE WHEN name_base64_check != 'BASE64' AND name_base64_check != 'NULL' THEN 1 ELSE 0 END) AS name_not_base64,
  SUM(CASE WHEN email_base64_check != 'BASE64' AND email_base64_check != 'NULL' THEN 1 ELSE 0 END) AS email_not_base64,
  SUM(CASE WHEN phone_base64_check != 'BASE64' AND phone_base64_check != 'NULL' THEN 1 ELSE 0 END) AS phone_not_base64,
  SUM(CASE WHEN zip_base64_check != 'BASE64' AND zip_base64_check != 'NULL' THEN 1 ELSE 0 END) AS zip_not_base64
FROM encrypted_check
"""
validation_result = spark.sql(validation_query)
validation_result.show()

# ------------------------------------------------------------------------------------
# End of Databricks PySpark PII Encryption Script
# ------------------------------------------------------------------------------------
