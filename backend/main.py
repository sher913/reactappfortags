import os
from os import environ
import logging
from logging.handlers import TimedRotatingFileHandler
import re
from avro.schema import NULL
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
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import TagSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from ingestion.ingest_api.helper.mce_convenience import (generate_json_output,
                                               get_sys_time,
                                               make_browsepath_mce,
                                               make_dataset_description_mce,
                                               make_dataset_urn,
                                               make_delete_mce,
                                               make_ownership_mce,
                                               make_platform, make_recover_mce,
                                               make_schema_mce, make_user_urn,make_tag_urn, make_schemaglobaltags_mce, 
                                               make_editableschema_mce,make_TagProperties_mce,make_dataset_editable_description_mce)
from ingestion.ingest_api.helper.models import (FieldParam, create_dataset_params,
                                      dataset_status_params, determine_type)
from datahub.emitter.rest_emitter import DatahubRestEmitter

app = FastAPI()

origins = [
    "http://localhost:3000/",
    "localhost:3000/"
]

#Change this endpoint depeding on ur datahub endpoint
datahub_gms_endpoint ="http://172.104.42.65:8080"




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
    Browse_Path: Optional[str]= None
    Dataset_Description: Optional[str]= None
    

@app.get('/getdatasets')
def main():
    elements=[]
    URL =datahub_gms_endpoint+"/entities"
    headers = {
    'Content-Type': 'application/json',
    'X-RestLi-Protocol-Version': '2.0.0'
            
    }
    parameters = {'action':'search'}

    data = '{ "input": "*", "entity": "dataset", "start": 0, "count": 1000}'
  
    response = requests.request("POST", URL, headers=headers, params = parameters, data=data)


    datasetobject =response.json()
    datasets = datasetobject["value"]["metadata"]["urns"]
    for dataset in datasets:
        keys_to_remove=[]
        aspect=getdatasetviaurn(dataset)

        for null_field in aspect:
            if not aspect[null_field]:
                keys_to_remove.append(null_field)

        for key in keys_to_remove:
            aspect.pop(key)

        elements.append(aspect)
    # for j in range (len(elements)):
    #     if "editableSchemaFieldInfo" in elements[j]["editableSchemaMetadata"]:
    #         print(True)
    
    
    return elements

def getdatasetviaurn(dataset):
        URL = datahub_gms_endpoint +"/entities/" +dataset
        headers = {
        'Content-Type': 'application/json',
        'X-RestLi-Protocol-Version': '2.0.0'
        }
        response = requests.request("GET", URL, headers=headers)
        newdatasetsnapshot=[]
        newdatasetsnapshot=dict.fromkeys(["DatasetKey","InstitutionalMemory","Ownership","UpstreamLineage","BrowsePaths","GlobalTags","editableSchemaMetadata","schemaMetadata", "DatasetProperties", "editableDatasetProperties"])
        datasetsnapshot =response.json()
        datasetsnapshot=datasetsnapshot['value']['com.linkedin.metadata.snapshot.DatasetSnapshot']
        # datasetsnapshot['aspects'].pop(0)
        datasetsnapshotAspects=datasetsnapshot['aspects']
        
        
        for s in range (len(datasetsnapshotAspects)):
            #For DatasetKey
            if "com.linkedin.metadata.key.DatasetKey" in datasetsnapshotAspects[s]:
                newdatasetsnapshot["DatasetKey"]=datasetsnapshotAspects[s]["com.linkedin.metadata.key.DatasetKey"]

            #For InstitutionalMemory              
            if "com.linkedin.common.InstitutionalMemory" in datasetsnapshotAspects[s]:
                newdatasetsnapshot["InstitutionalMemory"]=datasetsnapshotAspects[s]["com.linkedin.common.InstitutionalMemory"]
                
                
            #For Ownership
            if "com.linkedin.common.Ownership" in datasetsnapshotAspects[s]:
                newdatasetsnapshot["Ownership"]=datasetsnapshotAspects[s]["com.linkedin.common.Ownership"]

            #For UpstreamLineage
            if "com.linkedin.dataset.UpstreamLineage" in datasetsnapshotAspects[s]:
                newdatasetsnapshot["UpstreamLineage"]=datasetsnapshotAspects[s]["com.linkedin.dataset.UpstreamLineage"]

            #For BrowsePaths
            if "com.linkedin.common.BrowsePaths" in datasetsnapshotAspects[s]:
                newdatasetsnapshot["BrowsePaths"]=datasetsnapshotAspects[s]["com.linkedin.common.BrowsePaths"]


            #For GlobalTags
            if "com.linkedin.common.GlobalTags" in datasetsnapshotAspects[s]:
                newdatasetsnapshot["GlobalTags"]=datasetsnapshotAspects[s]["com.linkedin.common.GlobalTags"]


            #For EditableSchemaMetadata
            if "com.linkedin.schema.EditableSchemaMetadata" in datasetsnapshotAspects[s]:
                newdatasetsnapshot["editableSchemaMetadata"]=datasetsnapshotAspects[s]["com.linkedin.schema.EditableSchemaMetadata"]


            #For schemaMetadata
            if "com.linkedin.schema.SchemaMetadata" in datasetsnapshotAspects[s]:
                newdatasetsnapshot["schemaMetadata"]=datasetsnapshotAspects[s]["com.linkedin.schema.SchemaMetadata"]

            #For DatasetProperties
            if "com.linkedin.dataset.DatasetProperties" in datasetsnapshotAspects[s]:
                newdatasetsnapshot["DatasetProperties"]=datasetsnapshotAspects[s]["com.linkedin.dataset.DatasetProperties"]

            #For EditableDatasetProperties
            if "com.linkedin.dataset.EditableDatasetProperties" in datasetsnapshotAspects[s]:
                newdatasetsnapshot["editableDatasetProperties"]=datasetsnapshotAspects[s]["com.linkedin.dataset.EditableDatasetProperties"]
        
        return newdatasetsnapshot

       

       
        





    


@app.post('/getresult')
def getresult(Editeditems: List[EditedItem]):
    
    datasetEdited=[]
    for item in Editeditems:
        #extracts all edited unique datasets to use as for loops
        if item.Dataset_Name not in datasetEdited:
            datasetEdited.append(item.Dataset_Name)
       
        #makes the edited tags into a list for a fields and for BrowsePaths
        item.Editable_Tags= item.Editable_Tags.replace(" ", "")
        item.Editable_Tags= item.Editable_Tags.split(",")
        item.Original_Tags= item.Original_Tags.replace(" ", "")
        item.Original_Tags= item.Original_Tags.split(",")
        item.Global_Tags= item.Global_Tags.replace(" ", "")
        item.Global_Tags= item.Global_Tags.split(",")
        item.Browse_Path=item.Browse_Path.replace(" ", "")
        item.Browse_Path=item.Browse_Path.split(",")
    # print(datasetEdited)
    # print(Editeditems[0].Editable_Tags)
    # print(Editeditems[0].Original_Tags)
    requestor=make_user_urn("datahub")
    for dataset in datasetEdited:
        editablefield_params = []
        for item in Editeditems:
            if item.Dataset_Name == dataset:
                editable_field = {}
                editabletags = []
                browsePath=item.Browse_Path
                dataset_Description =item.Dataset_Description
                datasetName = make_dataset_urn(item.Platform_Name, item.Dataset_Name)
                editable_field["fieldPath"] = item.Field_Name
                editable_field["field_description"] = item.Description
                for editabletag in item.Editable_Tags:
                    if editabletag != '':
                        editabletags.append({"tag": make_tag_urn(editabletag)})
                        istagindataset(editabletag)

                
                editable_field["tags"]=editabletags
            
                editablefield_params.append(editable_field)
                #Clearing the fields is needed after appending, idk why the above for loop not clearing the fields
                editable_field={}
                editabletags = []

            
               
        originaldata(datasetName)
        platformName = originalplatformname
            
 

        
        
        creatoractor = originalschemadata["created"]["actor"]
        lastmodifiedactor = originalschemadata["lastModified"]["actor"]
        

        schemaName= originalschemadata["schemaName"]
        timeforschemametadata = originalschemadata["lastModified"]["time"]
        platformSchema = list(originalschemadata["platformSchema"].keys())[0]

        #putting '' as str, want to know if can use None
        documentSchema= ''
        tableSchema= ''
        rawSchema= ''
        schema= ''
        keySchema= ''
        valueSchema= ''
       
        
        #KafkaSchemaClass has 2 attr, documentSchema and keySchema, keySchema is optional
        if platformSchema == "com.linkedin.schema.KafkaSchema":
            documentSchema = originalschemadata["platformSchema"][platformSchema]["documentSchema"]
            

        
        #EspressoSchemaClass has 2 attr, documentSchema and tableSchema, both is required
        if platformSchema == "com.linkedin.schema.EspressoSchema":
            documentSchema = originalschemadata["platformSchema"][platformSchema]["documentSchema"]
            tableSchema = originalschemadata["platformSchema"][platformSchema]["tableSchema"]
                  


        #OracleDDLClass has 1 attr, tableSchema
        if platformSchema == "com.linkedin.schema.OracleDDL":
            tableSchema = originalschemadata["platformSchema"][platformSchema]["tableSchema"]
            


        #MySqlDDLClass has 1 attr, tableSchema
        if platformSchema == "com.linkedin.schema.MySqlDDL":
            tableSchema = originalschemadata["platformSchema"][platformSchema]["tableSchema"]
       
        
        #PrestoDDLClass has 1 attr, rawSchema
        if platformSchema == "com.linkedin.schema.PrestoDDL":
            rawSchema = originalschemadata["platformSchema"][platformSchema]["rawSchema"]
        
            
        
        #BinaryJsonSchemaClass has 1 attr, schema
        if platformSchema == "com.linkedin.schema.BinaryJsonSchema":
            schema = originalschemadata["platformSchema"][platformSchema]["schema"]            


        #OrcSchemaClass has 1 attr, schema
        if platformSchema == "com.linkedin.schema.OrcSchema":
            schema = originalschemadata["platformSchema"][platformSchema]["schema"]


        #SchemalessClass has no attr    


        #KeyValueSchemaClass has 2 attr, keySchema and valueSchema, both is required
        if platformSchema == "com.linkedin.schema.KeyValueSchema":
            keySchema = originalschemadata["platformSchema"][platformSchema]["keySchema"]
            valueSchema = originalschemadata["platformSchema"][platformSchema]["valueSchema"]
            


        #OtherSchemaClass has 1 attr, rawSchema
        if platformSchema == "com.linkedin.schema.OtherSchema":
            rawSchema = originalschemadata["platformSchema"][platformSchema]["rawSchema"]
            


   

        dataset_snapshot = DatasetSnapshot(
        urn=datasetName,
        aspects=[],
        )

        #Array Checker for if tags for schemametadata has been edited, tags are the only varaiable editable for schemametadata
        isSchemaMetadataChanged = []
        #Array to store SchemaMetaData field info
        field_params = []
        for existing_field in originalfields:
            current_field = {}
            schemametadatatags = []
            globaltags=[]
            current_field["fieldPath"] = existing_field["fieldPath"]
            #need to know if this is important [field_type]
            current_field["field_type"] = existing_field["nativeDataType"]

            if "nullable" in existing_field:
                current_field["nullable"]=existing_field["nullable"]
            
            if "recursive" in existing_field:
                current_field["recursive"]=existing_field["recursive"]

            if "description" not in existing_field:
                current_field["field_description"] = ""
                
            else: 
                current_field["field_description"] = existing_field["description"]
                
            for item in Editeditems:
                if item.Field_Name == existing_field["fieldPath"] and item.Dataset_Name == dataset:
                    #globaltag is not actually under schemadata aspect, it has its own aspect
                    for globaltag in item.Global_Tags:
                        if globaltag != '':
                            globaltags.append({"tag": make_tag_urn(globaltag)})
                            istagindataset(globaltag)
                    #for schemametadata aspects
                    for tag in item.Original_Tags:
                        if tag != '':
                            schemametadatatags.append({"tag": make_tag_urn(tag)})
                            istagindataset(tag)
            if schemametadatatags != []:
                current_field["tags"]=schemametadatatags
            #Filling the Array Checker for if tags for schemametadata has been edited, tags are the only varaiable editable for schemametadata
            if 'globalTags' in existing_field.keys():
                if 'tags' in current_field.keys():
                    if existing_field['globalTags']['tags'] !=current_field['tags']:
                        isSchemaMetadataChanged.append(True)
                else:
                    isSchemaMetadataChanged.append(True)
            elif 'tags' in current_field.keys():
                isSchemaMetadataChanged.append(True)
                
            print("exsiting:", existing_field)
            print(current_field)    
            current_field["type"]= list(existing_field["type"]['type'].keys())[0]
            field_params.append(current_field)
        
        OriDatasetAspects = getdatasetviaurn(datasetName)
    
        

        #Using Array Checker to decide whether to append aspect anot
        if True in isSchemaMetadataChanged:
            dataset_snapshot.aspects.append(
                make_schema_mce(
                dataset_urn=datasetName,
                platformName=platformName,

                platformSchema = platformSchema,
                documentSchema = documentSchema,
                schemaName=schemaName,
                tableSchema= tableSchema,
                rawSchema=rawSchema,
                schema=schema,
                keySchema = keySchema,
                valueSchema=valueSchema,

                creatoractor=creatoractor,
                lastmodifiedactor=lastmodifiedactor,
                fields=field_params,
                system_time=timeforschemametadata
            )
            )
        
        
        
        dataset_snapshot.aspects.append(
            make_browsepath_mce(
                path=browsePath
                )
                )


        
        dataset_snapshot.aspects.append(
            make_schemaglobaltags_mce(
                
                tags = globaltags
            )
        )
        
        
        
        #Checker for changes in editableDatasetProperties
        isDataset_Description_Changed = True
    
        if OriDatasetAspects['editableDatasetProperties'] is not None:
            if OriDatasetAspects['editableDatasetProperties']['description'] is not None:
                if OriDatasetAspects['editableDatasetProperties']['description']==dataset_Description:
                    isDataset_Description_Changed = False
                    
        elif dataset_Description =='':
            isDataset_Description_Changed = False

        if isDataset_Description_Changed == True:
            dataset_snapshot.aspects.append(
                            make_dataset_editable_description_mce(
                                requestor=requestor,
                                description= dataset_Description
                            )
                        )

        
        dataset_snapshot.aspects.append(
            make_editableschema_mce(
            #using datahub as requestor, change varaiable requestor if you are another user
            requestor=requestor,
            editablefields= editablefield_params

        )
        )
       

        

        
        metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
    
        # print(metadata_record)
        for mce in metadata_record.proposedSnapshot.aspects:
            if not mce.validate():
                rootLogger.error(
                    f"{mce.__class__} is not defined properly"
                )
                return Response(
                    f"Dataset was not created because dataset definition has encountered an error for {mce.__class__}",
                    status_code=400,
                )
        
        
               
               
        try:
            rootLogger.error(metadata_record)
            emitter = DatahubRestEmitter(datahub_gms_endpoint)
            emitter.emit_mce(metadata_record)
            emitter._session.close()
        except Exception as e:
            rootLogger.debug(e)
            return Response(
            "Dataset was not created because upstream has encountered an error {}".format(e),
            status_code=500,
        )
            
        rootLogger.info(
            "Make_dataset_request_completed_for {} requested_by {}".format(
                datasetName, requestor
        )
        )
    if(datasetEdited!=[]):
        return Response(
            "Datasets updated: {}\n\nrequested by: {}".format(
                datasetEdited, requestor
            ),
            status_code=201,
        )
    else:
        return Response(
            "No datasets were updated\n\nrequested by: {}".format(
               requestor
            ),
            status_code=201,
        )
        
             
    
    


@app.get('/originalresult')
def originaldata(urn):
    URL =datahub_gms_endpoint+"/entities/"+urn
    headers = {
    'Content-Type': 'application/json',
    'X-RestLi-Protocol-Version': '2.0.0'
    }
    global originalgmsdata
    global originalschemadata
    global originalfields
    global originalglobaltagsdata
    global originalplatformname
    response = requests.request("GET", URL, headers=headers)
   
    originalgmsdata =response.json()
   
    # originalgmsdata=originalgmsdata["value"]
    # originalgmsdata.update(DatasetSnapshotClass.dict())
    for s in range (len(originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"])):
        if "com.linkedin.schema.SchemaMetadata" in originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"][s]:
            originalschemadata= originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"][s]["com.linkedin.schema.SchemaMetadata"]

        if "com.linkedin.common.GlobalTags" in originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"][s]:
            originalglobaltagsdata= originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"][s]["com.linkedin.common.GlobalTags"]

        if "com.linkedin.metadata.key.DatasetKey" in originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"][s]:
            originalplatformname= originalgmsdata["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["aspects"][s]["com.linkedin.metadata.key.DatasetKey"]["platform"]

    originalfields = originalschemadata["fields"]

def istagindataset(tag):
    URL =datahub_gms_endpoint+"/entities"
    headers = {
    'Content-Type': 'application/json',
    'X-RestLi-Protocol-Version': '2.0.0'
            
    }
    parameters = {'action':'search'}

    data = '{ "input": "'+tag+'", "entity": "tag", "start": 0, "count": 10}'
  
    payload = requests.request("POST", URL, headers=headers, params = parameters, data=data)
    payload=payload.json()

    if(not payload["value"]['numEntities'] >= 1):
        tag_snapshot = TagSnapshot(
        urn=make_tag_urn(tag),
        aspects=[],
        )

        tag_snapshot.aspects.append(
            make_TagProperties_mce(
                name=tag
            )
        )
        tag_metadata_record = MetadataChangeEvent(proposedSnapshot=tag_snapshot)
        # print(metadata_record)
        for mce in tag_metadata_record.proposedSnapshot.aspects:
            if not mce.validate():
                rootLogger.error(
                    f"{mce.__class__} is not defined properly"
                )
                return Response(
                    f"Dataset was not created because dataset definition has encountered an error for {mce.__class__}",
                    status_code=400,
                )  
        try:
            rootLogger.error(tag_metadata_record)
            emitter = DatahubRestEmitter(datahub_gms_endpoint)
            emitter.emit_mce(tag_metadata_record)
            emitter._session.close()
        except Exception as e:
            rootLogger.debug(e)
            return Response(
            "Dataset was not created because upstream has encountered an error {}".format(e),
            status_code=500,
        )
            
        rootLogger.info(
            "Make_tag_request_completed_for {} requested_by {}".format(
                tag, "datahub"
        )
        )
    

        
    
  
    
   
    

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)






