FROM python:3.12-slim-bookworm

WORKDIR /app
COPY . /app

ENV PIP_ROOT_USER_ACTION=ignore
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
