import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datahub.metadata.schema_classes import DatasetSnapshotClass

import requests
from requests.api import request
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, Response, status
from starlette.datastructures import URL

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
from mce_convenience import (generate_json_output,
                                               get_sys_time,
                                               make_browsepath_mce,
                                               make_dataset_description_mce,
                                               make_dataset_urn,
                                               make_delete_mce,
                                               make_ownership_mce,
                                               make_platform, make_recover_mce,
                                               make_schema_mce, make_user_urn,make_tag_urn, make_schemaglobaltags_mce, 
                                               make_editableschema_mce,make_TagProperties_mce,make_dataset_editable_description_mce)
from models import (FieldParam, create_dataset_params,
                                      dataset_status_params, determine_type)
from datahub.emitter.rest_emitter import DatahubRestEmitter

app = FastAPI()

origins = [
    "http://localhost:3000/",
    "localhost:3000/"
]

#Change this endpoint depeding on ur datahub endpoint, uses http://localhost:8080 if not defined
datahub_gms_endpoint = os.getenv('datahub_gms_endpoint', 'http://172.104.42.65:8080')




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


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get('/getdatasets')
def main():
    elements=[]
    dropped_datasets=[]
    URL =datahub_gms_endpoint+"/entities"
    headers = {
    'Content-Type': 'application/json',
    'X-RestLi-Protocol-Version': '2.0.0'
            
    }
    parameters = {'action':'search'}
    start = 0
    #max is 10k, limited by GMS
    count =10000
    data = '{ "input": "*", "entity": "dataset", "start": ' +str(start)+',' '"count": ' + str(count)+'}'
  
    response = requests.request("POST", URL, headers=headers, params = parameters, data=data)
    datasetobject =response.json()
    #gets the total count of entities, in this case; datasets
    totalDatasetCount = datasetobject["value"]['numEntities']
    #Remove the amount of datasets already collected from total count
    totalDatasetCount-=count
    #extracts urns(datasets) to a list  called datasets
    datasets = datasetobject["value"]["metadata"]["urns"]
    #loop in case there are more than 10k datasets
    while totalDatasetCount > 0:
        #adds the count to start value, since index starts with 0, it works
        start+=count
        #redefined the data string
        data = '{ "input": "*", "entity": "dataset", "start": ' +str(start)+',' '"count": ' + str(count)+'}'
        response = requests.request("POST", URL, headers=headers, params = parameters, data=data)
        response = response.json()
        #adds the urns from response to datasets list
        datasets.extend(response["value"]["metadata"]["urns"])
         #Remove the amount collected datasets from total count
        totalDatasetCount-=count
    
    
  
    #Array for aspects that datasets must have else will be dropped
    required_aspects = ["SchemaMetadata","DatasetKey", "BrowsePaths"]
    for dataset in datasets:
        keys_to_remove=[]
        aspects=getdatasetviaurn(dataset)
        for null_field in aspects:
            if not aspects[null_field]:
                keys_to_remove.append(null_field)
        for key in keys_to_remove:
            aspects.pop(key)
        
        #Array to store aspects that are missing to show in console for user
        missing_aspects=[]
        #for loop and if condition to check if aspects have required_aspects, if no... else append to elements and send to REACT
        if not all(name in aspects for name in required_aspects):
            for aspect in required_aspects:
                if aspect not in aspects.keys():
                    missing_aspects.append(aspect)
            missing_aspects= ','.join(missing_aspects)
            dataset_missing_aspect= [dataset.split(',')[1]+" is missing: "+ missing_aspects]
            dropped_datasets.append(dataset_missing_aspect)
        else:
            elements.append(aspects)
    
    return [elements, dropped_datasets]

def getdatasetviaurn(dataset):
    URL = datahub_gms_endpoint +"/entities/" +dataset
    headers = {
    'Content-Type': 'application/json',
    'X-RestLi-Protocol-Version': '2.0.0'
    }
    response = requests.request("GET", URL, headers=headers)
    newdatasetsnapshot=[]
    newdatasetsnapshot=dict.fromkeys(["DatasetKey","InstitutionalMemory","Ownership","UpstreamLineage","BrowsePaths","GlobalTags","EditableSchemaMetadata","SchemaMetadata", "DatasetProperties", "EditableDatasetProperties"])
    datasetsnapshot =response.json()
    datasetsnapshot=datasetsnapshot['value']['com.linkedin.metadata.snapshot.DatasetSnapshot']
    datasetsnapshotAspects=datasetsnapshot['aspects']
    
    
    for aspect in datasetsnapshotAspects:
        for key in aspect.keys():
            keychecker = key.split('.')[-1]
            if keychecker in newdatasetsnapshot:
                newdatasetsnapshot[keychecker] = aspect[key]
    
    return newdatasetsnapshot

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
    #Your datahub account name, uses user_not_specified if not specified
    requestor=make_user_urn(os.getenv('actor', 'datahub'))

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

        

        OriDatasetAspects = getdatasetviaurn(datasetName)
        originalschemadata =  OriDatasetAspects["SchemaMetadata"]
        originalfields = OriDatasetAspects["SchemaMetadata"]["fields"]
        originalEditablefields= None        
        if OriDatasetAspects["EditableSchemaMetadata"] is not None:
            originalEditablefields =  OriDatasetAspects["EditableSchemaMetadata"]['editableSchemaFieldInfo']
            sorted(originalEditablefields, key=lambda originalEditablefields: originalEditablefields['fieldPath'])
        originalplatformname = OriDatasetAspects["DatasetKey"]["platform"]
        platformName = originalplatformname
        ##Sorting function, To match Schema and editableSchema
        sorted(originalfields, key=lambda originalfields: originalfields['fieldPath'])
        sorted(editablefield_params, key=lambda editablefield_params: editablefield_params['fieldPath'])
        
           
            

        
        
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
        print(originalschemadata["platformSchema"][platformSchema].keys())


   

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
                if existing_field['globalTags']['tags'] != current_field.get('tags',None):
                        isSchemaMetadataChanged.append(True)
            elif 'tags' in current_field.keys():
                isSchemaMetadataChanged.append(True)
             
            current_field["type"]= list(existing_field["type"]['type'].keys())[0]
            field_params.append(current_field)
        
        
    
        

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
        
        
        
        #Checker for changes in EditableDatasetProperties
        isDataset_Description_Changed = True
    
        if OriDatasetAspects['EditableDatasetProperties'] is not None:
            if OriDatasetAspects['EditableDatasetProperties']['description'] is not None:
                if OriDatasetAspects['EditableDatasetProperties']['description']==dataset_Description:
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

        
        

        #Array Checker for changes made in editableSchemametadata
        isEditableSchemaMetadataChanged=[]
        if originalEditablefields is not None:
            for f in range(len(originalEditablefields)):
                if originalEditablefields[f]['globalTags']['tags'] != editablefield_params[f]['tags'] or originalEditablefields[f]["description"] != editablefield_params[f]['field_description']:
                    isEditableSchemaMetadataChanged.append(True)
        else:
            for f in range(len(editablefield_params)):
                if editablefield_params[f]['tags'] or originalfields[f]['description'] != editablefield_params[f]['field_description']:
                    isEditableSchemaMetadataChanged.append(True)

        if True in isEditableSchemaMetadataChanged:
            dataset_snapshot.aspects.append(
                make_editableschema_mce(
                #using datahub as requestor, change varaiable requestor if you are another user
                requestor=requestor,
                editablefields= editablefield_params

            )
            )
       

        

        
    #     metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
    
        
    #     for mce in metadata_record.proposedSnapshot.aspects:
    #         if not mce.validate():
    #             rootLogger.error(
    #                 f"{mce.__class__} is not defined properly"
    #             )
    #             return Response(
    #                 f"Dataset was not created because dataset definition has encountered an error for {mce.__class__}",
    #                 status_code=400,
    #             )
        
        
               
               
    #     try:
    #         rootLogger.error(metadata_record)
    #         emitter = DatahubRestEmitter(datahub_gms_endpoint)
    #         emitter.emit_mce(metadata_record)
    #         emitter._session.close()
    #     except Exception as e:
    #         rootLogger.debug(e)
    #         return Response(
    #         "Dataset was not created because upstream has encountered an error {}".format(e),
    #         status_code=500,
    #     )
            
    #     rootLogger.info(
    #         "Make_dataset_request_completed_for {} requested_by {}".format(
    #             datasetName, requestor
    #     )
    #     )
    # if(datasetEdited!=[]):
    #     return Response(
    #         "Datasets updated: {}\n\nrequested by: {}".format(
    #             datasetEdited, requestor
    #         ),
    #         status_code=201,
    #     )
    # else:
    #     return Response(
    #         "No datasets were updated\n\nrequested by: {}".format(
    #            requestor
    #         ),
    #         status_code=201,
    #     )
        
    

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)






