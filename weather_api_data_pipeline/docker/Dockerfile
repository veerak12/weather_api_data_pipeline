# Use the mageai/mageai:latest base image
FROM mageai/mageai:latest

# Set the USER_CODE_PATH argument to the current working directory
ARG USER_CODE_PATH=/app

# Set the working directory to the USER_CODE_PATH
WORKDIR ${USER_CODE_PATH}

# Copy requirements.txt to the USER_CODE_PATH
COPY requirements.txt ${USER_CODE_PATH}/requirements.txt

# Copy citinames.csv to the /home/pipeline directory in the container
COPY D:/data_zoomcamp_2024/pipeline/citinames.csv /home/pipeline

# Install Python dependencies from requirements.txt
RUN pip3 install -r ${USER_CODE_PATH}/requirements.txt

