FROM python:3.7 as backend-build-deps
WORKDIR /
COPY main.py mce_convenience.py models.py requirements.txt /
RUN pip install -U pip      
RUN python3 -m pip install -r requirements.txt
RUN mkdir -p log
CMD ["python", "main.py"]