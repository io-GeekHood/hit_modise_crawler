FROM python:3.9-slim-buster
RUN mkdir -p /app/state_storage
WORKDIR /app
ENV MONGODB_URI=$MONGODB_URI
ENV SLEEPING=$SLEEPING
ENV PROXY_PROVIDER=$PROXY_PROVIDER
ENV MODISE_DB=$MODISE_DB
ENV LOCAL=$LOCAL
COPY requirements.txt requirements.txt
RUN python3.9 -m pip install --upgrade pip
RUN python3.9 -m pip install -r requirements.txt
COPY modise_json.py modise_json.py
ENTRYPOINT ["python3.9", "modise_json.py"]
