"""Convenience functions for creating MCEs"""
import datetime
import json
import logging
import os
import re
import time
from typing import Dict, List, Optional, Type, TypeVar, Union

from datahub.ingestion.api import RecordEnvelope
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import *
from requests.api import options

log = logging.getLogger(__name__)

DEFAULT_ENV = "PROD"
DEFAULT_FLOW_CLUSTER = "prod"

T = TypeVar("T")   


def get_sys_time() -> int:
    return int(time.time() * 1000)


def make_dataset_urn(platform: str, name: str, env: str = DEFAULT_ENV) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


def make_path(platform: str, name: str, env: str = DEFAULT_FLOW_CLUSTER) -> str:
    return f"/{env}/{platform}/{name}"


def make_platform(platform: str) -> str:
    return f"urn:li:dataPlatform:{platform}"


def make_user_urn(username: str) -> str:
    return f"urn:li:corpuser:{username}"


def make_tag_urn(tag: str) -> str:
    return f"urn:li:tag:{tag}"


def make_institutionalmemory_mce(
    dataset_urn: str, input_url: List[str], input_description: List[str], actor: str
) -> InstitutionalMemoryClass:
    """
    returns a list of Documents
    """
    sys_time = get_sys_time()
    actor = make_user_urn(actor)
    mce = InstitutionalMemoryClass(
        elements=[
            InstitutionalMemoryMetadataClass(
                url=url,
                description=description,
                createStamp=AuditStampClass(
                    time=sys_time,
                    actor=actor,
                ),
            )
            for url, description in zip(input_url, input_description)
        ]
    )

    return mce


def make_browsepath_mce(
    path: List[str],
) -> BrowsePathsClass:
    """
    Creates browsepath for dataset. By default, if not specified, Datahub assigns it to /prod/platform/datasetname
    """
    mce = BrowsePathsClass(paths=path)
    return mce


def make_lineage_mce(
    upstream_urns: List[str],
    downstream_urn: str,
    actor: str,
    lineage_type: str = Union[
        DatasetLineageTypeClass.TRANSFORMED,
        DatasetLineageTypeClass.COPY,
        DatasetLineageTypeClass.VIEW,
    ],
) -> MetadataChangeEventClass:
    """
    Specifies Upstream Datasets relative to this dataset. Downstream is always referring to current dataset
    urns should be created using make_dataset_urn
    lineage have to be one of the 3
    """
    sys_time = get_sys_time()
    actor = actor
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=downstream_urn,
            aspects=[
                UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            auditStamp=AuditStampClass(
                                time=sys_time,
                                actor=actor,
                            ),
                            dataset=upstream_urn,
                            type=lineage_type,
                        )
                        for upstream_urn in upstream_urns
                    ]
                )
            ],
        )
    )
    return mce


def make_dataset_description_mce(
    dataset_name: str,
    description: str,
    externalUrl: str = None,
    tags: List[str] = [],
    customProperties: Optional[Dict[str, str]] = None,
) -> DatasetPropertiesClass:
    """
    Tags and externalUrl doesnt seem to have any impact on UI.
    """
    return DatasetPropertiesClass(
        description=description,
        externalUrl=externalUrl,
        customProperties=customProperties,
    )


def make_dataset_editable_description_mce(
    requestor: str,
    description: str,
) -> EditableDatasetPropertiesClass:
    sys_time = get_sys_time()

    mce = EditableDatasetPropertiesClass(
        description=description,
        created=AuditStampClass(time=sys_time, actor=requestor),
        lastModified=AuditStampClass(time=sys_time, actor=requestor),
    )
    return mce


def make_TagProperties_mce(name: str, description: Optional[str]) -> TagPropertiesClass:

    return TagPropertiesClass(name=name, description = description if description else None)


def make_schemaglobaltags_mce(
    tags: List[str] = None,
) -> GlobalTagsClass:

    return GlobalTagsClass(tags=tags)


def make_editableschema_mce(
    requestor: str, editablefields: List[Dict[str, str]]
) -> MetadataChangeEventClass:
    sys_time = get_sys_time()
    mce = EditableSchemaMetadataClass(
        # using datahub as requestor, change varaiable requestor in main.py(FASTAPI) if you are another user
        created=AuditStampClass(time=sys_time, actor=requestor),
        lastModified=AuditStampClass(time=sys_time, actor=requestor),
        editableSchemaFieldInfo=[
            EditableSchemaFieldInfoClass(
                fieldPath=field["fieldPath"],
                description=field["field_description"],
                globalTags=GlobalTagsClass(tags=field.get("tags")),
            )
            for field in editablefields
        ],
    )
    return mce


def make_schema_mce(
    dataset_urn: str,
    platformName: str,
    schemaName: str,
    platformSchema: str,
    creatoractor: str,
    lastmodifiedactor: str,
    fields: List[Dict[str, str]],
    primaryKeys: List[str] = None,
    foreignKeysSpecs: List[str] = None,
    system_time: int = None,
) -> MetadataChangeEventClass:
    if system_time:
        try:
            sys_time = system_time
        except ValueError as e:
            log.error("specified_time is out of range")
            sys_time = get_sys_time()
    else:
        sys_time = get_sys_time()

    for item in fields:
        item["nativeType"] = item.get("field_type", "")
        item["type"] = {
            "com.linkedin.schema.BooleanType": BooleanTypeClass(),
            "com.linkedin.schema.StringType": StringTypeClass(),
            "com.linkedin.schema.FixedType": FixedTypeClass(),
            "com.linkedin.schema.BytesType": BytesTypeClass(),
            "com.linkedin.schema.NumberType": NumberTypeClass(),
            "com.linkedin.schema.DateType": DateTypeClass(),
            "com.linkedin.schema.TimeType": TimeTypeClass(),
            "com.linkedin.schema.EnumType": EnumTypeClass(),
            "com.linkedin.schema.NullType": NullTypeClass(),
            "com.linkedin.schema.MapType": MapTypeClass(),
            "com.linkedin.schema.ArrayType": ArrayTypeClass(),
            "com.linkedin.schema.UnionType": UnionTypeClass(),
            "com.linkedin.schema.RecordType": RecordTypeClass(),
        }.get(item["type"])

    # Reverse engineering the platformschema to send to gms under schemametadata
    platformSchemaKey = list(platformSchema.keys())[0]
    platformSchemaValue = platformSchema[platformSchemaKey]
    platformSchema = {
        "com.linkedin.schema.KafkaSchema": KafkaSchemaClass(
            documentSchema=platformSchemaValue.get("documentSchema")
        ),
        "com.linkedin.schema.EspressoSchema": EspressoSchemaClass(
            documentSchema=platformSchemaValue.get("documentSchema"),
            tableSchema=platformSchemaValue.get("tableSchema"),
        ),
        "com.linkedin.schema.OracleDDL": OracleDDLClass(
            tableSchema=platformSchemaValue.get("tableSchema")
        ),
        "com.linkedin.schema.MySqlDDL": MySqlDDLClass(
            tableSchema=platformSchemaValue.get("tableSchema")
        ),
        "com.linkedin.schema.PrestoDDL": PrestoDDLClass(
            rawSchema=platformSchemaValue.get("rawSchema")
        ),
        "com.linkedin.schema.BinaryJsonSchema": BinaryJsonSchemaClass(
            schema=platformSchemaValue.get("schema")
        ),
        "com.linkedin.schema.OrcSchema": OrcSchemaClass(
            schema=platformSchemaValue.get("schema")
        ),
        "com.linkedin.schema.Schemaless": SchemalessClass(),
        "com.linkedin.schema.KeyValueSchema": KeyValueSchemaClass(
            keySchema=platformSchemaValue.get("keySchema"),
            valueSchema=platformSchemaValue.get("valueSchema"),
        ),
        "com.linkedin.schema.OtherSchema": OtherSchemaClass(
            rawSchema=platformSchemaValue.get("rawSchema")
        ),
    }.get(platformSchemaKey)

    mce = SchemaMetadataClass(
        schemaName,
        platform=platformName,
        version=0,
        # Modfied to record both last modified actor and creator
        created=AuditStampClass(time=sys_time, actor=creatoractor),
        lastModified=AuditStampClass(time=get_sys_time(), actor=lastmodifiedactor),
        hash="",
        platformSchema=platformSchema,
        fields=[
            SchemaFieldClass(
                fieldPath=item["fieldPath"],
                type=SchemaFieldDataTypeClass(type=item["type"]),
                nativeDataType=item.get("nativeType", ""),
                description=item.get("field_description", ""),
                nullable=item.get("nullable", None),
                globalTags=GlobalTagsClass(tags=item.get("tags"))
                if item.get("tags")
                else None,
                recursive=item.get("recursive", None),
            )
            for item in fields
        ],
        primaryKeys=primaryKeys,  # no visual impact in UI
        foreignKeysSpecs=None,
    )
    return mce


def make_ownership_mce(actor: str, dataset_urn: str) -> OwnershipClass:
    return OwnershipClass(
        owners=[
            OwnerClass(
                owner=actor,
                type=OwnershipTypeClass.DATAOWNER,
            )
        ],
        lastModified=AuditStampClass(
            time=int(time.time() * 1000),
            actor=make_user_urn(actor),
        ),
    )


def generate_json_output(mce: MetadataChangeEventClass, file_loc: str) -> None:
    """
    Generates the json MCE files that can be ingested via CLI. For debugging
    """
    mce_obj = mce.to_obj()
    file_name = mce.proposedSnapshot.urn.replace(
        "urn:li:dataset:(urn:li:dataPlatform:", ""
    ).split(",")[1]
    path = os.path.join(file_loc, f"{file_name}.json")

    with open(path, "w") as f:
        json.dump(mce_obj, f, indent=4)


def make_delete_mce(
    dataset_name: str,
) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name, aspects=[StatusClass(removed=True)]
        )
    )


def make_recover_mce(
    dataset_name: str,
) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name, aspects=[StatusClass(removed=False)]
        )
    )
