# Use an official Python runtime as a parent image
FROM python:3.9-slim

USER root

# Install required system packages
RUN apt search libgeos

RUN apt-get update && \
    apt-get install -y \
    iputils-ping \
    r-base \
    libproj-dev \
    proj-bin \
    proj-data \
    libgeos++-dev\
    g++\
    unixodbc \
    unixodbc-dev\
    poppler-data \
    locales locales-all\
    poppler-utils &&\
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /home/admin/jose/app

# Copy the current directory contents into the container
COPY . .

RUN echo "ab" && ls -la /

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 8150 for external access
EXPOSE 8150

# Run app.py when the container launches
CMD ["python", "app.py"]
