import os
import logging
from logging.handlers import TimedRotatingFileHandler
import re
from avro.schema import NULL
from datahub.metadata.schema_classes import DatasetSnapshotClass, NullTypeClass

import requests
from requests.api import request
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, Response, status
from starlette.datastructures import URL

from starlette.responses import JSONResponse
from pydantic import BaseModel
import datetime
from typing import List, Optional, Dict, Any
import json
import socket

socket.getaddrinfo("localhost", 8080)

from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import TagSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from mce_convenience import (
    generate_json_output,
    get_sys_time,
    make_browsepath_mce,
    make_dataset_description_mce,
    make_dataset_urn,
    make_delete_mce,
    make_ownership_mce,
    make_platform,
    make_recover_mce,
    make_schema_mce,
    make_user_urn,
    make_tag_urn,
    make_schemaglobaltags_mce,
    make_editableschema_mce,
    make_TagProperties_mce,
    make_dataset_editable_description_mce,
)
from models import (
    FieldParam,
    create_dataset_params,
    dataset_status_params,
    determine_type,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter

app = FastAPI()

origins = ["http://localhost:3000/", "localhost:3000/"]

# Change this endpoint depeding on ur datahub endpoint, uses http://localhost:8080 if not defined
datahub_gms_endpoint = os.getenv("datahub_gms_endpoint", "http://172.104.42.65:8080")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

DEBUG = False
rootLogger = logging.getLogger("__name__")
logformatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
rootLogger.setLevel(logging.DEBUG)

streamLogger = logging.StreamHandler()
streamLogger.setFormatter(logformatter)
streamLogger.setLevel(logging.DEBUG)
rootLogger.addHandler(streamLogger)

if not DEBUG:
    log = TimedRotatingFileHandler(
        "./log/api.log", when="midnight", interval=1, backupCount=14
    )
    log.setLevel(logging.DEBUG)
    log.setFormatter(logformatter)
    rootLogger.addHandler(log)

rootLogger.info("started!")


class EditedItem(BaseModel):
    ID: int
    Origin: str
    Platform_Name: str
    Dataset_Name: str
    Global_Tags: Optional[Dict[Any, Any]] = None
    Field_Name: str
    Editable_Tags: Optional[Dict[Any, Any]] = None
    Original_Tags: Optional[Dict[Any, Any]] = None
    Description: Optional[str] = None
    Browse_Path: Optional[List[str]] = None
    Dataset_Description: Optional[str] = None


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/getdatasets")
def main():
    global All_Tags
    elements = []
    dropped_datasets = []
    URL = datahub_gms_endpoint + "/entities"
    headers = {"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"}
    parameters = {"action": "search"}
    start = 0
    # max is 10k, limited by GMS
    count = 10000
    data = (
        '{ "input": "*", "entity": "dataset", "start": ' + str(start) + ","
        '"count": ' + str(count) + "}"
    )

    response = requests.request(
        "POST", URL, headers=headers, params=parameters, data=data
    )
    datasetobject = response.json()
    # gets the total count of entities, in this case; datasets
    totalDatasetCount = datasetobject["value"]["numEntities"]
    # Remove the amount of datasets already collected from total count
    totalDatasetCount -= count
    # extracts urns(datasets) to a list  called datasets
    dataset_unfiltered_list = datasetobject["value"]["entities"]
    datasets = []
    for unfiltered_list in dataset_unfiltered_list:
        datasets.append(unfiltered_list["entity"])

    # loop in case there are more than 10k datasets
    while totalDatasetCount > 0:
        # adds the count to start value, since index starts with 0, it works
        start += count
        # redefined the data string
        data = (
            '{ "input": "*", "entity": "dataset", "start": ' + str(start) + ","
            '"count": ' + str(count) + "}"
        )
        response = requests.request(
            "POST", URL, headers=headers, params=parameters, data=data
        )
        response = response.json()
        # adds the urns from response to datasets list
        dataset_unfiltered_list = response["value"]["entities"]
        for unfiltered_list in dataset_unfiltered_list:
            datasets.append(unfiltered_list["entity"])
        # Remove the amount collected datasets from total count
        totalDatasetCount -= count
    # Array for aspects that datasets must have else will be dropped
    required_aspects = ["SchemaMetadata", "DatasetKey", "BrowsePaths"]
    for dataset in datasets:
        keys_to_remove = []
        aspects = getdatasetviaurn(dataset)
        for null_field in aspects:
            if not aspects[null_field]:
                keys_to_remove.append(null_field)
        for key in keys_to_remove:
            aspects.pop(key)

        # Array to store aspects that are missing to show in console for user
        missing_aspects = []
        # for loop and if condition to check if aspects have required_aspects, if no... else append to elements and send to REACT
        if not all(name in aspects for name in required_aspects):
            for aspect in required_aspects:
                if aspect not in aspects.keys():
                    missing_aspects.append(aspect)
            missing_aspects = ",".join(missing_aspects)
            dataset_missing_aspect = [
                dataset.split(",")[1] + " is missing: " + missing_aspects
            ]
            dropped_datasets.append(dataset_missing_aspect)
        else:
            elements.append(aspects)
    All_Tags = getalltags()
    return [elements, dropped_datasets, All_Tags]


def getalltags():
    URL = datahub_gms_endpoint + "/entities"
    headers = {"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"}
    parameters = {"action": "search"}
    start = 0
    # max is 10k, limited by GMS
    count = 10000
    data = (
        '{ "input": "*", "entity": "tag", "start": ' + str(start) + ","
        '"count": ' + str(count) + "}"
    )
    response = requests.request(
        "POST", URL, headers=headers, params=parameters, data=data
    )
    response = response.json()
    totalTagsCount = response["value"]["numEntities"]
    totalTagsCount -= count
    AllTags = []
    # response['value']['entities'] == list of entites retrieved
    for tags in response["value"]["entities"]:
        AllTags.append(tags["entity"])
    while totalTagsCount > 0:
        # adds the count to start value, since index starts with 0, it works
        start += count
        # redefined the data string
        data = (
            '{ "input": "*", "entity": "tag", "start": ' + str(start) + ","
            '"count": ' + str(count) + "}"
        )
        response = requests.request(
            "POST", URL, headers=headers, params=parameters, data=data
        )
        response = response.json()
        # adds the urns from response to datasets list
        for tags in response["value"]["entities"]:
            AllTags.append(tags["entity"])
        # Remove the amount collected datasets from total count
        totalTagsCount -= count
    cleanedTagsObject = {}
    for tag in AllTags:
        cleanedtag = tag.split("urn:li:tag:").pop()
        res = requests.request(
            "GET",
            datahub_gms_endpoint
            + "/aspects/urn%3Ali%3Atag%3A"
            + cleanedtag
            + "?aspect=tagProperties&version=0",
        )
        res = res.json()
        # ask xl if any way to combine 3 .gets() into 1
        tag_Description = res.get("aspect")
        if tag_Description:
            tag_Description = tag_Description.get("com.linkedin.tag.TagProperties")
            tag_Description = tag_Description.get("description")
        cleanedTagsObject[cleanedtag] = {
            "Tag": cleanedtag,
            "Description": tag_Description if tag_Description else "",
            "Count": 0,
        }
    return cleanedTagsObject


def getdatasetviaurn(dataset):
    URL = datahub_gms_endpoint + "/entities/" + dataset
    headers = {"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"}
    response = requests.request("GET", URL, headers=headers)
    newdatasetsnapshot = []
    newdatasetsnapshot = dict.fromkeys(
        [
            "DatasetKey",
            "InstitutionalMemory",
            "Ownership",
            "UpstreamLineage",
            "BrowsePaths",
            "GlobalTags",
            "EditableSchemaMetadata",
            "SchemaMetadata",
            "DatasetProperties",
            "EditableDatasetProperties",
        ]
    )
    datasetsnapshot = response.json()

    try:
        datasetsnapshot = datasetsnapshot["value"][
            "com.linkedin.metadata.snapshot.DatasetSnapshot"
        ]
    except KeyError:
        return {}
    datasetsnapshotAspects = datasetsnapshot["aspects"]

    for aspect in datasetsnapshotAspects:
        for key in aspect.keys():
            keychecker = key.split(".")[-1]
            if keychecker in newdatasetsnapshot:
                newdatasetsnapshot[keychecker] = aspect[key]
    return newdatasetsnapshot


def addTagtoGms(tag, description=None):
    tag_snapshot = TagSnapshot(
        urn=make_tag_urn(tag),
        aspects=[],
    )

    tag_snapshot.aspects.append(
        make_TagProperties_mce(name=tag, description=description)
    )
    tag_metadata_record = MetadataChangeEvent(proposedSnapshot=tag_snapshot)
    for mce in tag_metadata_record.proposedSnapshot.aspects:
        if not mce.validate():
            rootLogger.error(f"{mce.__class__} is not defined properly")
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
            "Dataset was not created because upstream has encountered an error {}".format(
                e
            ),
            status_code=500,
        )

    rootLogger.info(
        "Make_tag_request_completed_for {} requested_by {}".format(tag, "datahub")
    )


@app.post("/updatetag")
def updatetags(Editedtags: Dict[Any, Any] = None):
    tags_modified = []
    tags_not_modified = []
    for value in Editedtags.values():
        tag = value["Tag"]
        description = value["Description"]

        if tag not in All_Tags.keys() or description != All_Tags[tag]["Description"]:
            addTagtoGms(tag, description)
            tags_modified.append(tag)
        else:
            tags_not_modified.append(tag)
    if tags_modified != []:
        return Response(
            "Tags updated: {}".format(tags_modified),
            status_code=201,
        )
    else:
        return Response(
            "No datasets or tags were updated because tags {} already exist".format(
                tags_not_modified
            ),
            status_code=201,
        )


@app.post("/getresult")
def getresult(Editeditems: List[EditedItem]):

    datasetEdited = []
    for item in Editeditems:
        # extracts all edited unique datasets to use as for loops
        if item.Dataset_Name not in datasetEdited:
            datasetEdited.append(item.Dataset_Name)

    # Your datahub account name, uses user_not_specified if not specified
    requestor = make_user_urn(os.getenv("actor", "datahub"))
    for dataset in datasetEdited:
        editablefield_params = []
        for item in Editeditems:
            if item.Dataset_Name == dataset:
                editable_field = {}
                editabletags = []
                browsePath = item.Browse_Path
                dataset_Description = item.Dataset_Description
                datasetName = make_dataset_urn(item.Platform_Name, item.Dataset_Name)
                editable_field["fieldPath"] = item.Field_Name
                editable_field["field_description"] = item.Description
                for editabletag in item.Editable_Tags:
                    editabletag_tag = item.Editable_Tags[editabletag].get("Tag", "")
                    editable_tag_description = item.Editable_Tags[editabletag].get(
                        "Description", None
                    )
                    if editabletag_tag != "":
                        editabletags.append({"tag": make_tag_urn(editabletag_tag)})
                        if (
                            editabletag_tag not in All_Tags.keys()
                            or All_Tags[editabletag_tag]["Description"]
                            != editable_tag_description
                        ):
                            print(
                                "print added new editableTag",
                                editabletag_tag,
                                "with description of",
                                editable_tag_description,
                            )
                            addTagtoGms(editabletag_tag, editable_tag_description)
                            All_Tags[editabletag_tag] = {
                                "Tag": editabletag_tag,
                                "Description": editable_tag_description,
                                "Count": 0,
                            }

                editable_field["tags"] = editabletags

                editablefield_params.append(editable_field)
                # Clearing the fields is needed after appending, idk why the above for loop not clearing the fields
                editable_field = {}
                editabletags = []

        OriDatasetAspects = getdatasetviaurn(datasetName)
        originalschemadata = OriDatasetAspects["SchemaMetadata"]
        originalfields = OriDatasetAspects["SchemaMetadata"]["fields"]
        originalEditablefields = None
        if OriDatasetAspects["EditableSchemaMetadata"] is not None:
            originalEditablefields = OriDatasetAspects["EditableSchemaMetadata"][
                "editableSchemaFieldInfo"
            ]
            sorted(
                originalEditablefields,
                key=lambda originalEditablefields: originalEditablefields["fieldPath"],
            )
        originalplatformname = OriDatasetAspects["DatasetKey"]["platform"]
        platformName = originalplatformname
        ##Sorting function, To match Schema and editableSchema
        sorted(originalfields, key=lambda originalfields: originalfields["fieldPath"])
        sorted(
            editablefield_params,
            key=lambda editablefield_params: editablefield_params["fieldPath"],
        )

        # fields needed to feed schema mce in mce_convinence.py
        creatoractor = originalschemadata["created"]["actor"]
        lastmodifiedactor = originalschemadata["lastModified"]["actor"]
        schemaName = originalschemadata["schemaName"]
        timeforschemametadata = originalschemadata["lastModified"]["time"]
        platformSchema = originalschemadata["platformSchema"]

        dataset_snapshot = DatasetSnapshot(
            urn=datasetName,
            aspects=[],
        )

        # Array Checker for if tags for schemametadata has been edited, tags are the only varaiable editable for schemametadata
        isSchemaMetadataChanged = []
        # Array to store SchemaMetaData field info
        field_params = []
        for existing_field in originalfields:
            current_field = {}
            schemametadatatags = []
            globaltags = []
            current_field["fieldPath"] = existing_field["fieldPath"]
            # need to know if this is important [field_type]
            current_field["field_type"] = existing_field["nativeDataType"]

            if "nullable" in existing_field:
                current_field["nullable"] = existing_field["nullable"]

            if "recursive" in existing_field:
                current_field["recursive"] = existing_field["recursive"]

            if "description" not in existing_field:
                current_field["field_description"] = ""

            else:
                current_field["field_description"] = existing_field["description"]

            for item in Editeditems:
                if (
                    item.Field_Name == existing_field["fieldPath"]
                    and item.Dataset_Name == dataset
                ):
                    # globaltag is not actually under schemadata aspect, it has its own aspect
                    for globaltag in item.Global_Tags:
                        globaltag_tag = item.Global_Tags[globaltag].get("Tag", "")
                        globaltag_description = item.Global_Tags[globaltag].get(
                            "Description", None
                        )
                        if globaltag_tag != "":
                            globaltags.append({"tag": make_tag_urn(globaltag_tag)})
                            if (
                                globaltag_tag not in All_Tags.keys()
                                or All_Tags[globaltag_tag]["Description"]
                                != globaltag_description
                            ):
                                print("print added new editableTag", globaltag_tag)
                                addTagtoGms(globaltag_tag, globaltag_description)
                                All_Tags[globaltag_tag] = {
                                    "Tag": globaltag_tag,
                                    "Description": globaltag_description,
                                    "Count": 0,
                                }
                    # for schemametadata aspects
                    for tag in item.Original_Tags:
                        tag_tag = item.Original_Tags[tag].get("Tag", "")
                        tag_description = item.Original_Tags[tag].get(
                            "Description", None
                        )
                        if tag_tag != "":
                            schemametadatatags.append({"tag": make_tag_urn(tag_tag)})
                            if (
                                tag_tag not in All_Tags.keys()
                                or All_Tags[tag_tag]["Description"] != tag_description
                            ):
                                print("print added new editableTag", tag_tag)
                                addTagtoGms(tag_tag, tag_description)
                                All_Tags[tag_tag] = {
                                    "Tag": tag_tag,
                                    "Description": tag_description,
                                    "Count": 0,
                                }
            if schemametadatatags != []:
                current_field["tags"] = schemametadatatags
            # Filling the Array Checker for if tags for schemametadata has been edited, tags are the only varaiable editable for schemametadata
            if "globalTags" in existing_field.keys():
                if existing_field["globalTags"]["tags"] != current_field.get(
                    "tags", None
                ):
                    isSchemaMetadataChanged.append(True)
            elif "tags" in current_field.keys():
                isSchemaMetadataChanged.append(True)

            current_field["type"] = list(existing_field["type"]["type"].keys())[0]
            field_params.append(current_field)

        # Using Array Checker to decide whether to append aspect anot
        if True in isSchemaMetadataChanged:
            dataset_snapshot.aspects.append(
                make_schema_mce(
                    dataset_urn=datasetName,
                    platformName=platformName,
                    platformSchema=platformSchema,
                    schemaName=schemaName,
                    creatoractor=creatoractor,
                    lastmodifiedactor=lastmodifiedactor,
                    fields=field_params,
                    system_time=timeforschemametadata,
                )
            )

        dataset_snapshot.aspects.append(make_browsepath_mce(path=browsePath))

        dataset_snapshot.aspects.append(make_schemaglobaltags_mce(tags= globaltags))

        # Checker for changes in EditableDatasetProperties
        isDataset_Description_Changed = True

        if OriDatasetAspects["EditableDatasetProperties"] is not None:
            if (
                OriDatasetAspects["EditableDatasetProperties"]["description"]
                is not None
            ):
                if (
                    OriDatasetAspects["EditableDatasetProperties"]["description"]
                    == dataset_Description
                ):
                    isDataset_Description_Changed = False

        elif dataset_Description == "":
            isDataset_Description_Changed = False

        if isDataset_Description_Changed == True:
            dataset_snapshot.aspects.append(
                make_dataset_editable_description_mce(
                    requestor=requestor, description=dataset_Description
                )
            )

        # Array Checker for changes made in editableSchemametadata
        isEditableSchemaMetadataChanged = []
        print(originalEditablefields)
        if originalEditablefields is not None:
            for f in range(len(originalEditablefields)):
                if (
                    originalEditablefields[f]["globalTags"]["tags"]
                    != editablefield_params[f]["tags"]
                ):
                    isEditableSchemaMetadataChanged.append(True)
                try: 
                    if (originalEditablefields[f]["description"]
                    != editablefield_params[f]["field_description"]):
                        isEditableSchemaMetadataChanged.append(True)
                    
                except KeyError:
                    if editablefield_params[f]["field_description"]:
                        isEditableSchemaMetadataChanged.append(True)
        else:
            for f in range(len(editablefield_params)):
                if (
                    editablefield_params[f]["tags"]
                ):
                    isEditableSchemaMetadataChanged.append(True)
                try: 
                    if (originalfields[f]["description"]
                    != editablefield_params[f]["field_description"]):
                        isEditableSchemaMetadataChanged.append(True)
                    
                except KeyError:
                    if editablefield_params[f]["field_description"]:
                        isEditableSchemaMetadataChanged.append(True)

        if True in isEditableSchemaMetadataChanged:
            dataset_snapshot.aspects.append(
                make_editableschema_mce(
                    # using datahub as requestor, change varaiable requestor if you are another user
                    requestor=requestor,
                    editablefields=editablefield_params,
                )
            )

        metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

        for mce in metadata_record.proposedSnapshot.aspects:
            if not mce.validate():
                rootLogger.error(f"{mce.__class__} is not defined properly")
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
                "Dataset was not created because upstream has encountered an error {}".format(
                    e
                ),
                status_code=500,
            )

        rootLogger.info(
            "Make_dataset_request_completed_for {} requested_by {}".format(
                datasetName, requestor
            )
        )
    if datasetEdited != []:
        return Response(
            "Datasets updated: {}\n\nrequested by: {}".format(datasetEdited, requestor),
            status_code=201,
        )
    else:
        return Response(
            "No datasets were updated\n\nrequested by: {}".format(requestor),
            status_code=201,
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
