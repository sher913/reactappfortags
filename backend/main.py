import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request
from starlette.datastructures import URL
import requests


app = FastAPI()

origins = [
    "http://localhost:3000/",
    "localhost:3000/"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"]
)
#VM address for hosting datahub
URL ="http://172.104.42.65:8080/datasets"
headers = {
'Content-Type': 'application/json',
'X-RestLi-Protocol-Version': '2.0.0',
          'X-RestLi-Method': 'finder'
}
parameters = {'q':'search',
                'input':'*'}

@app.get('/getdatasets')
def main():
  
    response = requests.request("GET", URL, headers=headers, params = parameters)
    datasetobject =response.json()
    return datasetobject

@app.get('/getresult')
def main():
  
    response = ["fred","adad"]
    result =response
    return result  

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)



