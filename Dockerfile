FROM python:3.9-slim-buster
RUN mkdir -p /app/state_storage
RUN mkdir -p /app/refrences
WORKDIR /app
COPY modise_json.py modise_json.py
COPY modise_image.py modise_image.py
ENV MONGODB_URI=mongodb://hit_admin:*5up3r53CUR3D@mongodb:27017
ENV MODISE_DB=modise-main
ENV AWS_HOST=nginx:9002
ENV S3_HOST=http://nginx:9002/
ENV AWS_ACCESS=minioadmin
ENV AWS_SECRET=sghllkfij,dhvrndld
ENV PROXY_PROVIDER=http://collector_proxy:8000/proxy
ENV MDS_DB=modise-main
ENV TOROB_COLLECTOR_MODE=Products
ENV TRY_UPLOAD=10
ENV BOTTLENECK=8
ENV LOCAL=false
ENV SLEEPING=18
COPY requirements.txt requirements.txt
RUN python3.9 -m pip install --upgrade pip
RUN python3.9 -m pip install -r requirements.txt
ENTRYPOINT ["python3.9", "modise_json.py"]
