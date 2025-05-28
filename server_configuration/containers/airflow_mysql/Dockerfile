
# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.8.2

#install otel collector for newrelic
# RUN pip install newrelic-airflow-plugin

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
    poppler-utils &&\
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


COPY requirements.txt /opt/airflow/
RUN echo "Installing dependencies from requirements.txt..." && \
    pip install --upgrade pip setuptools wheel &&\
    pip install  -r /opt/airflow/requirements.txt && \
    echo "Dependency installation completed."

RUN pip install --upgrade --no-binary shapely shapely
RUN pip install Cartopy


