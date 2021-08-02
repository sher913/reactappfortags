from requests.api import request
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, Response, status
from starlette.datastructures import URL
import requests
from starlette.responses import JSONResponse
from pydantic import BaseModel
import datetime
from typing import List, Optional

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

class Item(BaseModel):
    ID: int
    Platform_Name: str
    Dataset_Name: str
    Global_Tags: Optional[str]= None
    Tags_For_Field: Optional[str]= None
    Description: Optional[str]= None
    Date_Modified: int




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

@app.post('/getresult')
def getresult(items: List[Item]):
   return items
    
    


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)



