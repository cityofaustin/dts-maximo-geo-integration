#!/usr/bin/env python3

import os
import glob
import pytz
import boto3
import email
import tempfile
from email import policy
from operator import itemgetter
from datetime import datetime


def get_most_recent_file(bucket_name, prefix):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator('list_objects_v2')

    files = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            files.extend(page["Contents"])

    if not files:
        return "No files found in the bucket with the specified prefix."

    # Sort the files by last modified date
    files = sorted(files, key=itemgetter("LastModified"), reverse=True)

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

    # List of headers to check
    headers_to_check = {
        'X-SES-Spam-Verdict': 'PASS',
        'X-SES-Virus-Verdict': 'PASS',
        'Received-SPF': 'pass',
        'X-OriginatorOrg': 'austintexas.gov'
    }

    # Check the specified email headers
    for header, expected_value in headers_to_check.items():
        actual_value = parsed_email.get(header)
        if actual_value is None or expected_value not in actual_value:
            print(f"Email validation condition not met: {header} is {actual_value}. Expected: {expected_value}")
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


def upload_attachments_to_s3(bucket, prefix, location):
    s3 = boto3.resource("s3")

    # Create a new folder with the current date and time in UTC
    folder_name = datetime.now(pytz.utc).strftime("%Y%m%d-%H%M%S_UTC")
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
