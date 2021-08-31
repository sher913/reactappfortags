import os
from os import environ
import logging
from logging.handlers import TimedRotatingFileHandler
from datahub.metadata.schema_classes import DatasetSnapshotClass


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

from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from ingestion.ingest_api.helper.mce_convenience import (generate_json_output,
                                               get_sys_time,
                                               make_browsepath_mce,
                                               make_dataset_description_mce,
                                               make_dataset_urn,
                                               make_delete_mce,
                                               make_ownership_mce,
                                               make_platform, make_recover_mce,
                                               make_schema_mce, make_user_urn)
from ingestion.ingest_api.helper.models import (FieldParam, create_dataset_params,
                                      dataset_status_params, determine_type)
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
    
    

    












@app.get('/getdatasets')
def main():
    URL ="http://172.104.42.65:8080/datasets"
    headers = {
    'Content-Type': 'application/json',
    'X-RestLi-Protocol-Version': '2.0.0',
            'X-RestLi-Method': 'finder'
    }
    parameters = {'q':'search',
                    'input':'*'}
  
    response = requests.request("GET", URL, headers=headers, params = parameters)
   
    datasetobject =response.json()
   
    
    return datasetobject

#datafromGMS=main() #to get orignial json data from gms
#print(datafromGMS)  


@app.get('/getresult')
def getresult(Editeditems: List[EditedItem]):
    originaldata()
    rest_endpoint = "http://172.104.42.65:8080"   
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

               

                properties = {
                "dataset_origin": "Copied from XL's ingest API, need check how to use this",
                "dataset_location": "Copied from XL's ingest API, need check how to use this"}

                dataset_description = ""

                dataset_snapshot = DatasetSnapshot(
                urn=datasetName,
                aspects=[],
                )

                dataset_snapshot.aspects.append(
                make_dataset_description_mce(
                    dataset_name=datasetName,
                    description=dataset_description,
                    customProperties=properties,
                    )
                )

                dataset_snapshot.aspects.append(make_ownership_mce(actor=requestor, dataset_urn=datasetName))
                dataset_snapshot.aspects.append(make_browsepath_mce(dataset_urn=datasetName, path=[browsePath]))
                
                current_field = {}
                current_field["fieldPath"] = item.dict().get('Field_Name')
                #need to know if this is important [field_type]
                current_field["field_type"] = "boolean"
                if "Description" not in item:
                    current_field["field_description"] = ""
                else: 
                    current_field["field_description"] = item.dict().get("Description")
                field_params.append(current_field)
                
               
        # try:
        #     emitter = DatahubRestEmitter(rest_endpoint)

        #     for mce in all_mce:
        #         emitter.emit_mce(mce)   
        #     emitter._session.close()
        # except Exception as e:
        #     rootLogger.debug(e)
        #     return Response("Dataset was not created because upstream has encountered an error {}".format(e), status_code=502)
        # rootLogger.info("Make_dataset_request_completed_for {} requested_by {}".format(item.dataset_name, item.dataset_owner))      
        # return Response(content = "dataset can be found at {}/dataset/{}".format(datahub_url, make_dataset_urn(item.dataset_type, item.dataset_name)),
        #                 status_code = 205) 
             
   
    
#    #empty orginal tags are == ''
#     print(Editeditems[0].Original_Tags== '')

    
#     print(editedtags[0])
#     return(Editeditems)

@app.get('/originalresult')
def originaldata():
    URL ="http://172.104.42.65:8080/entities/urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"
    headers = {
    'Content-Type': 'application/json',
    'X-RestLi-Protocol-Version': '2.0.0'
    }
    global originalgmsdata
  
    response = requests.request("GET", URL, headers=headers)
   
    originalgmsdata =response.json()
   
    # originalgmsdata=originalgmsdata["value"]
    # originalgmsdata.update(DatasetSnapshotClass.dict())
    for s in range (len(originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"])):
        if "com.linkedin.schema.SchemaMetadata" in originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"][s]:
            originalschemadata= originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"][s]
        else:
            continue

   
    # originalschemadata = tuple(originalschemadata) doesnt work, its in a unhashable list
    for i in originalschemadata:
        print(i)
    return originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"][3]["com.linkedin.schema.SchemaMetadata"]["fields"]
    
    

    


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)






