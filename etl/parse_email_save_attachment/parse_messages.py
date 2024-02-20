#!/usr/bin/env python3

import os
import pytz
import boto3
import email
import tempfile
from email import policy
from operator import itemgetter
from datetime import datetime


def get_most_recent_file(bucket_name, prefix):
    s3 = boto3.client("s3")

    # List objects within the specified bucket and prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Check if the bucket is empty or the prefix doesn't match any files
    if "Contents" not in response:
        return "No files found in the bucket with the specified prefix."

    # Sort the files by last modified date
    files = sorted(response["Contents"], key=itemgetter("LastModified"), reverse=True)

    # Get the most recent file
    most_recent_file = files[0]
    return most_recent_file["Key"]


def get_file_content(bucket_name, file_key):
    s3 = boto3.client("s3")

    # Get the object
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)

    # Get the object's content
    content = obj["Body"].read().decode("utf-8")
    return content


def parse_email_from_s3(email_content):
    # Decode the string into bytes
    email_content_bytes = email_content.encode("utf-8")
    parsed_email = email.message_from_bytes(email_content_bytes, policy=policy.default)

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


import glob


def upload_attachments_to_s3(bucket, prefix, location):
    s3 = boto3.resource("s3")

    # Create a new folder with the current date and time in Central Time
    central = pytz.timezone("US/Central")
    folder_name = datetime.now(central).strftime("%Y%m%d-%H%M%S")
    new_prefix = os.path.join(prefix, folder_name)

    # List all files in the location
    csv_files = glob.glob(os.path.join(location, "*.csv"))
    xlsx_files = glob.glob(os.path.join(location, "*.xlsx"))
    data_files = sorted(csv_files + xlsx_files)

    if data_files:
        # Take the alphabetically first CSV or XLSX file
        data_file = data_files[0]
        file_extension = os.path.splitext(data_file)[1]

        # Create a new key for the data file
        data_key = os.path.join("attachments", f"most_recent_data{file_extension}")

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


def main():
    # Replace with your bucket name and path
    bucket = "emergency-mgmt-recd-data"
    read_prefix = "emails-received/"
    write_prefix = "attachments/"

    most_recent_file = get_most_recent_file(bucket, read_prefix)
    print(f"The most recently uploaded file is: {most_recent_file}")

    content = get_file_content(bucket, most_recent_file)
    # print(f"The content of the most recently uploaded file is: {content}")

    location = parse_email_from_s3(content)
    print(f"The attachments are saved in: {location}")

    upload_attachments_to_s3(bucket, write_prefix, location)
    print(f"The attachments are uploaded to S3.")


if __name__ == "__main__":
    main()
