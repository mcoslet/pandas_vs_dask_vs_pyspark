FROM python:3.10
COPY . /app
WORKDIR /app
ENV PYTHONPATH /app:$PYTHONPATH
COPY requirements.txt .

# Install OpenJDK-17, java is needed for spark
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean;

# Set JAVA_HOME environment variable, needed for spark
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]