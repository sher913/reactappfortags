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
from logging.handlers import TimedRotatingFileHandler
import logging
from ingestion.ingest_api.helper.models import FieldParam, create_dataset_params, dataset_status_params, determine_type
from ingestion.ingest_api.helper.mce_convenience import make_delete_mce, make_schema_mce, make_dataset_urn, \
                    make_user_urn, make_dataset_description_mce, make_recover_mce, \
                    make_browsepath_mce, make_ownership_mce, make_platform, get_sys_time 
from datahub.emitter.rest_emitter import DatahubRestEmitter

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

DEBUG = False
rootLogger = logging.getLogger("__name__")
logformatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s')
rootLogger.setLevel(logging.DEBUG)

streamLogger = logging.StreamHandler()
streamLogger.setFormatter(logformatter)
streamLogger.setLevel(logging.DEBUG)
rootLogger.addHandler(streamLogger)

if not DEBUG:
    log = TimedRotatingFileHandler('./log/api.log', when='midnight', interval=1, backupCount=14)
    log.setLevel(logging.DEBUG)
    log.setFormatter(logformatter)
    rootLogger.addHandler(log)    

rootLogger.info("started!")

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
    datasetEdited=[]
    for item in Editeditems:
        #extracts all edited unique datasets to use as for loops
        if item.Dataset_Name not in datasetEdited:
            datasetEdited.append(item.Dataset_Name)
       
        #makes the edited tags into a list for a fields
        item.Editable_Tags= item.Editable_Tags.replace(" ", "")
        item.Editable_Tags= item.Editable_Tags.split(",")
    print(datasetEdited)
    print(Editeditems[0].Editable_Tags)
    for dataset in datasetEdited:
        field_params = []
        for item in Editeditems:
            if item.Dataset_Name == dataset:
                datasetName = make_dataset_urn(item.Platform_Name, item.Dataset_Name)
                platformName = make_platform(item.Platform_Name)  
                browsePath = "/{}/{}".format(item.Platform_Name, item.Dataset_Name) 

                requestor = make_user_urn("datahub")

                properties = {"dataset_origin": item.Origin}#, 
                                #"dataset_location": item.dict().get("dataset_location", "")}  

                all_mce=[]
                all_mce.append(make_dataset_description_mce(dataset_name = datasetName, 
                                                            customProperties=properties
                                                            ))    

                all_mce.append(make_ownership_mce(actor = requestor, 
                                                    dataset_urn = datasetName))   

                all_mce.append(make_browsepath_mce(dataset_urn=datasetName, 
                                                    path=[browsePath]))  

                
              #need help formatting this
                current_field={}
                current_field.update(existing_field.dict())          
                current_field["fieldPath"]  = current_field.pop("field_name")
                if "field_description" not in current_field:
                    current_field["field_description"] = ""
                field_params.append(current_field)
            all_mce.append(make_schema_mce(dataset_urn = datasetName,
                                            platformName = platformName,
                                            actor = requestor,
                                            fields = field_params,
                                            ))  
        try:
            emitter = DatahubRestEmitter(rest_endpoint)

            for mce in all_mce:
                emitter.emit_mce(mce)   
            emitter._session.close()
        except Exception as e:
            rootLogger.debug(e)
            return Response("Dataset was not created because upstream has encountered an error {}".format(e), status_code=502)
        rootLogger.info("Make_dataset_request_completed_for {} requested_by {}".format(item.dataset_name, item.dataset_owner))      
        return Response(content = "dataset can be found at {}/dataset/{}".format(datahub_url, make_dataset_urn(item.dataset_type, item.dataset_name)),
                        status_code = 205) 
    
#    #empty orginal tags are == ''
#     print(Editeditems[0].Original_Tags== '')

    
#     print(editedtags[0])
    return(Editeditems)

@app.post('/originalresult')
def orginaldata(Originalitems: List[OriginalItem]):
    global orignaldatafromgms
    orignaldatafromgms = Originalitems
    return orignaldatafromgms #switch the return to something else after everything working
    #print(Originalitems)
    


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)



