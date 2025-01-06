#!/usr/bin/env python3

# Standard library imports
import glob
import hashlib
import os
import tempfile
from datetime import datetime
from operator import itemgetter

# Related third party imports
import boto3
import email
import pytz
from email import policy


def get_most_recent_file(bucket_name, prefix):
    """
    Get the most recent file in a specified S3 bucket and prefix.

    This function lists all the objects in the specified S3 bucket and prefix,
    sorts them by the last modified date in descending order, and returns the key
    of the most recent file.

    Parameters:
    bucket_name (str): The name of the S3 bucket.
    prefix (str): The prefix (folder path) to look for files.

    Returns:
    str: The key (path) of the most recent file. If no files are found, it returns a message.
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    files = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            files.extend(page["Contents"])

    if not files:
        raise FileNotFoundError(
            "No files found in the bucket with the specified prefix."
        )

    # Sort the files by last modified date
    files = sorted(files, key=itemgetter("LastModified"), reverse=True)

    # Get the most recent file
    most_recent_file = files[0]
    return most_recent_file["Key"]


def get_file_content(bucket_name, file_key):
    """
    Get the content and SHA256 hash of a file in a specified S3 bucket and key.

    This function retrieves the specified file from the S3 bucket, reads its content,
    computes the SHA256 hash of the content, and decodes the content from bytes to a string.

    Parameters:
    bucket_name (str): The name of the S3 bucket.
    file_key (str): The key (path) of the file in the S3 bucket.

    Returns:
    tuple: A tuple where the first element is the content of the file as a string,
           and the second element is the SHA256 hash of the content.
    """
    s3 = boto3.client("s3")

    # Get the object
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)

    # Get the object's content
    content = obj["Body"].read()

    # Compute the SHA256 hash of the content
    sha256_hash = hashlib.sha256(content).hexdigest()

    # Decode the content
    content = content.decode("utf-8")

    return content, sha256_hash


def parse_email_from_s3(email_content):
    """
    Parse an email from S3 and check specific headers.

    This function decodes the email content, checks specific headers against expected values
    to help validate the sender's identity, and saves any attachments to a
    temporary directory. If any of the headers do not meet the expected values, it prints
    a message and quits.

    Parameters:
    email_content (str): The content of the email as a string.

    Returns:
    str: The path to the temporary directory where attachments are saved.
    """

    # Decode the string into bytes
    email_content_bytes = email_content.encode("utf-8")
    parsed_email = email.message_from_bytes(email_content_bytes, policy=policy.default)

    # List of headers to check
    headers_to_check = {
        "X-SES-Spam-Verdict": "PASS",
        "X-SES-Virus-Verdict": "PASS",
        "Received-SPF": "pass",
        "X-OriginatorOrg": "austintexas.gov",
    }

    # Check the specified email headers
    for header, expected_value in headers_to_check.items():
        actual_value = parsed_email.get(header)
        if actual_value is None or expected_value not in actual_value:
            print(
                f"Email validation condition not met: {header} is {actual_value}. Expected: {expected_value}"
            )
            quit()

    # Create a temporary directory to store attachments
    temp_dir = tempfile.mkdtemp()
    if parsed_email.is_multipart():
        # If email has multiple parts (like attachments or different content types)
        for part in parsed_email.iter_parts():
            if part.is_attachment():
                # Save the attachment in the temporary directory
                filename = part.get_filename()
                if filename:
                    filepath = os.path.join(temp_dir, filename)
                    with open(filepath, "wb") as f:
                        f.write(part.get_payload(decode=True))

    return temp_dir


def upload_attachments_to_s3(bucket, prefix, location, hash):
    """
    Upload attachments to a specified S3 bucket and prefix.

    This function creates a new folder in the S3 bucket with the current date, time, and hash.
    It then uploads all the files in the specified location to this new folder. If there are any
    XLSX files in the location, it takes the alphabetically first one, deletes any existing
    "most recent data" files in the S3 bucket, and uploads this file as the new "most recent data" file.

    Parameters:
    bucket (str): The name of the S3 bucket.
    prefix (str): The prefix (folder path) in the S3 bucket where to create the new folder.
    location (str): The local directory where the files to upload are located.
    hash (str): The hash to include in the name of the new folder.

    Returns:
    None
    """
    s3 = boto3.resource("s3")

    # Create a new folder with the current date and time in UTC
    folder_name = datetime.now(pytz.utc).strftime("%Y%m%d-%H%M%S_UTC") + "-" + hash[:8]
    new_prefix = os.path.join(prefix, folder_name)

    # List all XLSX files in the location
    xlsx_files = glob.glob(os.path.join(location, "*.xlsx"))
    data_files = sorted(xlsx_files)

    if data_files:
        # Take the alphabetically first XLSX file
        data_file = data_files[0]

        # Create a new key for the data file
        data_key = os.path.join("attachments", "most_recent_data.xlsx")

        # Delete all existing "most recent data" files
        for obj in s3.Bucket(bucket).objects.filter(
            Prefix="attachments/most_recent_data"
        ):
            s3.Object(bucket, obj.key).delete()

        # Upload the data file to S3
        s3.meta.client.upload_file(data_file, bucket, data_key)

    # Upload all files to the new folder in S3
    for root, dirs, files in os.walk(location):
        for file in files:
            file_path = os.path.join(root, file)

            # Create a new key for each file
            key = os.path.join(new_prefix, file)

            # Upload the file to the new folder in S3
            s3.meta.client.upload_file(file_path, bucket, key)


def check_if_hash_has_been_seen(hash, bucket, prefix):
    """
    Check if a hash has been seen before in a specified S3 bucket and prefix.

    This function lists all the objects in the specified S3 bucket and prefix,
    and checks if the first 8 characters of the hash are in the name of any of the objects.
    If they are, it prints a message and returns True. If not, it returns False. The result
    of this function are used to pick if the program exists early or continues.

    Parameters:
    hash (str): The hash to check.
    bucket (str): The name of the S3 bucket.
    prefix (str): The prefix (folder path) in the S3 bucket where to look for the hash.

    Returns:
    bool: True if the hash has been seen before, False otherwise.
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    short_hash = hash[:8]  # Only use the first 8 characters of the hash

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                if short_hash in obj["Key"]:
                    print(
                        f"Hash {short_hash} has been seen before in file {obj['Key']}"
                    )
                    return True

    print(f"Hash {short_hash} has not been seen before.")
    return False


def main():
    bucket = "emergency-mgmt-recd-data"
    read_prefix = "emails-received/"
    write_prefix = "attachments/"

    most_recent_file = get_most_recent_file(bucket, read_prefix)
    print(f"The most recently uploaded file is: {most_recent_file}")

    content, hash = get_file_content(bucket, most_recent_file)
    # print(f"The content of the most recently uploaded file is: {content}")
    print(f"The SHA256 hash of the content is: {hash}")

    if check_if_hash_has_been_seen(hash, bucket, write_prefix):
        quit()

    location = parse_email_from_s3(content)
    print(f"The attachments are saved in: {location}")

    upload_attachments_to_s3(bucket, write_prefix, location, hash)
    print(f"The attachments are uploaded to S3.")


if __name__ == "__main__":
    main()
