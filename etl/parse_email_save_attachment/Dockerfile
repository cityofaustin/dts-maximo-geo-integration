FROM python:3.12-bookworm

RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get install -y aptitude vim magic-wormhole

# Copy contents of the current folder into /application/
COPY . /application/

# Set working directory
WORKDIR /application/

# Install dependencies
RUN pip install -r requirements.txt

# Set entry point to /bin/bash
ENTRYPOINT ["/bin/bash"]
