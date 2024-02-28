# ETL

## Intent

![Diagram](etl/parse_email_save_attachment/docs/diagram.png)

This ETL provides a mechanism to receive data via email for the geo team's FME ETL to consume.

## Mechanism

This ETL works in conjunction with AWS infrastructure. An email address has been allocated to receive emails with attachments. The raw email is stored in S3, which is accessed by this script, and the script then decodes the attachments out of that email. Any attachments that are CSV or XLSX documents are stored in a particular location in S3, and the geo team's FME ETL is configured to consume from that location. Additionally, the script will also store the attachments in a dated folder so they can be accessed even after a more recent email has been received.

## Usage

```bash
docker compose build # you can ignore this if you want and docker compose will build if you don't have a copy of the image locally
docker compose run --entrypoint=/bin/bash parse-email # this will get you a shell to work on the container
docker compose up # this will run the container, and execute the ETL and then exit
```
