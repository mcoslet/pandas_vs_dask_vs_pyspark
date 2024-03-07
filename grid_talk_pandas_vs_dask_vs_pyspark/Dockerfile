FROM python:3.10
COPY . /app
WORKDIR /app
ENV PYTHONPATH /app:$PYTHONPATH
COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]