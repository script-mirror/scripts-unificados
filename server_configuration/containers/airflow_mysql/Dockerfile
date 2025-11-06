# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.8.2

# Switch to root user for installing packages
USER root

# Install required system packages
RUN apt-get update && \
    apt-get install -y \
    r-base \
    libproj-dev \
    proj-bin \
    proj-data \
    g++\
    unixodbc \
    unixodbc-dev\
    poppler-data \
    locales locales-all\
    poppler-utils \
    git &&\
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set timezone
ENV TZ=America/Sao_Paulo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && dpkg-reconfigure -f noninteractive tzdata

# Set locale environment variables
ENV LANG pt_BR.UTF-8
ENV LANGUAGE pt_BR:pt
ENV LC_ALL pt_BR.UTF-8

RUN R -e "install.packages('readxl', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('minpack.lm', repos='https://cran.rstudio.com/')"
RUN R -e "install.packages('lpSolve', repos='https://cran.rstudio.com/')"

# Switch back to the airflow user
USER airflow

# Add GitHub credentials as build args
ARG GIT_USERNAME
ARG GIT_TOKEN

COPY requirements.txt /opt/airflow/

# Configure Git credentials BEFORE pip install
RUN git config --global credential.helper store && \
    echo "https://${GIT_USERNAME}:${GIT_TOKEN}@github.com" > ~/.git-credentials

RUN echo "Installing dependencies from requirements.txt..." && \
    pip install --upgrade pip setuptools<67.0.0 wheel &&\
    pip install -r /opt/airflow/requirements.txt && \
    echo "Dependency installation completed."