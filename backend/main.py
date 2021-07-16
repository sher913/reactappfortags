import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request
from starlette.datastructures import URL
import requests
import json

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

URL ="http://localhost:8080/datasets"
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

  

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)



