FROM apache/airflow:3.0.0 

# If remove /, requirements.txt will be copied to /opt/airflow instead
COPY requirements.txt /requirements.txt 

RUN pip install --upgrade pip 
RUN pip install "apache-airflow==3.0.0" -r /requirements.txt 

USER root 
RUN apt-get update && \
	apt-get install -y procps && \
	apt-get install -y default-jre && \
	apt-get clean 

USER airflow 