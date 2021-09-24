FROM python:3.7-slim-buster as builder
COPY backend/main.py backend/mce_convenience.py backend/models.py backend/requirements.txt ./
RUN pip install -U pip      
RUN python3 -m pip install -r requirements.txt
RUN mkdir -p log
EXPOSE 8000
CMD ["python", "main.py"]