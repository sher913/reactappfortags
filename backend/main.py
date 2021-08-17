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
import json
import socket
socket.getaddrinfo('localhost', 8080)
# from ingestion.ingest_api.helper.models import FieldParam, create_dataset_params, dataset_status_params, determine_type
# from ingestion.ingest_api.helper.mce_convenience import make_delete_mce, make_schema_mce, make_dataset_urn, \
#                     make_user_urn, make_dataset_description_mce, make_recover_mce, \
#                     make_browsepath_mce, make_ownership_mce, make_platform, get_sys_time 
# from datahub.emitter.rest_emitter import DatahubRestEmitter

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

class EditedItem(BaseModel):
    ID: int
    Origin: str
    Platform_Name: str
    Dataset_Name: str
    Global_Tags: Optional[str]= None
    Field_Name: str
    Editable_Tags: Optional[str]= None
    Original_Tags: Optional[str]= None
    Description: Optional[str]= None
    

class OriginalItem(BaseModel):
    ID: int
    Origin: str
    Platform_Name: str
    Dataset_Name: str
    Global_Tags: List[Optional[str]]= None
    Field_Name: str
    Editable_Tags: List[Optional[str]]= None
    Original_Tags: List[Optional[str]]= None
    Description: Optional[str]= None
    
    









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

#datafromGMS=main() #to get orignial json data from gms
#print(datafromGMS)  


@app.post('/getresult')
def getresult(Editeditems: List[EditedItem]):
    # datasetName = make_dataset_urn(Editeditems[0].Platform_Name, Editeditems[0].Dataset_Name)
    # print("{}_{}".format(Editeditems[0].Dataset_Name,str(get_sys_time())))
    print(Editeditems[0].Original_Tags== '')
   

@app.post('/originalresult')
def orginaldata(Originalitems: List[OriginalItem]):
    global orignaldatafromgms
    orignaldatafromgms = Originalitems
    return orignaldatafromgms #switch the return to something else after everything working
    #print(Originalitems)
    


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)



