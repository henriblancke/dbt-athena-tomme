import agate
import re
import os
import boto3
import tempfile
from botocore.exceptions import ClientError
from itertools import chain
from threading import Lock
from typing import Dict, Iterator, Optional, Set
from uuid import uuid4

from dbt.adapters.base import available
from dbt.adapters.base.impl import GET_CATALOG_MACRO_NAME
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.relation import AthenaRelation, AthenaSchemaSearchMap
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import Manifest
from dbt.events import AdapterLogger

logger = AdapterLogger("Athena")

boto3_client_lock = Lock()


class AthenaAdapter(SQLAdapter):
    ConnectionManager = AthenaConnectionManager
    Relation = AthenaRelation

    def _get_boto_session(self, profile: str, region: str) -> boto3.Session:
        return boto3.Session(profile_name=profile, region_name=region)

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "int"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    def _parse_lf_tags(self, lf_tags_expr: str) -> Dict[str, str]:
        lf_tags = {}
        if lf_tags_expr:
            lf_tags = dict([tag.split("=") for tag in lf_tags_expr.split(",")])

        return lf_tags

    @available
    def add_lf_tags_to_table(
        self, database: str, table: str, lf_tags_expr: Optional[str] = None
    ):
        conn = self.connections.get_thread_connection()
        client = conn.handle
        session = self._get_boto_session(client.profile_name, client.region_name)

        lf_tags = self._parse_lf_tags(lf_tags_expr)
        lf_client = session.client("lakeformation")

        if lf_tags:
            lf_client.add_lf_tags_to_resource(
                Resource={
                    "Table": {
                        "DatabaseName": database,
                        "Name": table,
                    },
                },
                LFTags=[
                    {
                        "TagKey": key,
                        "TagValues": [
                            value,
                        ],
                    }
                    for key, value in lf_tags.items()
                ],
            )

    @available
    def add_lf_tags_to_database(self, database: str):
        conn = self.connections.get_thread_connection()
        client = conn.handle
        session = self._get_boto_session(client.profile_name, client.region_name)

        lf_tags_expr = conn.credentials.lf_tags
        lf_tags = self._parse_lf_tags(lf_tags_expr)
        lf_client = session.client("lakeformation")

        if lf_tags:
            lf_client.add_lf_tags_to_resource(
                Resource={
                    "Database": {"Name": database},
                },
                LFTags=[
                    {
                        "TagKey": key,
                        "TagValues": [
                            value,
                        ],
                    }
                    for key, value in lf_tags.items()
                ],
            )

    @available
    def s3_uuid_table_location(self):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        return f"{client.s3_staging_dir}tables/{str(uuid4())}/"

    @available
    def clean_up_partitions(
        self, database_name: str, table_name: str, where_condition: str
    ):
        # Look up Glue partitions & clean up
        conn = self.connections.get_thread_connection()
        client = conn.handle
        session = self._get_boto_session(client.profile_name, client.region_name)

        with boto3_client_lock:
            glue_client = session.client("glue")
        s3_resource = session.resource("s3")
        partitions = glue_client.get_partitions(
            # CatalogId='123456789012', # Need to make this configurable if it is different from default AWS Account ID
            DatabaseName=database_name,
            TableName=table_name,
            Expression=where_condition,
        )
        p = re.compile("s3://([^/]*)/(.*)")
        for partition in partitions["Partitions"]:
            logger.debug(
                "Deleting objects for partition '{}' at '{}'",
                partition["Values"],
                partition["StorageDescriptor"]["Location"],
            )
            m = p.match(partition["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()

    @available
    def clean_up_table(self, database_name: str, table_name: str):
        # Look up Glue partitions & clean up
        conn = self.connections.get_thread_connection()
        client = conn.handle
        session = self._get_boto_session(client.profile_name, client.region_name)

        with boto3_client_lock:
            glue_client = session.client("glue")
        try:
            table = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                logger.debug("Table '{}' does not exists - Ignoring", table_name)
                return

        if table is not None:
            logger.debug(
                "Deleting table data from'{}'",
                table["Table"]["StorageDescriptor"]["Location"],
            )
            p = re.compile("s3://([^/]*)/(.*)")
            m = p.match(table["Table"]["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)
                s3_resource = session.resource("s3")
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()

    @available
    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:
        return super().quote_seed_column(column, False)

    @available
    def upload_seed_to_s3(self, database_name: str, table_name:str, table: agate.Table) -> str:
        conn = self.connections.get_thread_connection()
        client = conn.handle
        session = self._get_boto_session(client.profile_name, client.region_name)

        path = f"seeds/{database_name}/{table_name}"
        file_name = f"{path}/{table_name}.json"

        bucket = client.s3_staging_dir.replace("s3://", "").split("/")[0]
        s3_client = session.client("s3")

        with boto3_client_lock:
            # cross-platform support vs tempfile.NamedTemporaryFile
            tmpfile = os.path.join(tempfile.gettempdir(), os.urandom(24).hex())
            table.to_json(tmpfile, newline=True)
            s3_client.upload_file(tmpfile, bucket, file_name)
            os.remove(tmpfile)

        return f"s3://{bucket}/{path}"

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Dict[str, Optional[Set[str]]],
        manifest: Manifest,
    ) -> agate.Table:

        kwargs = {"information_schema": information_schema, "schemas": schemas}
        table = self.execute_macro(
            GET_CATALOG_MACRO_NAME,
            kwargs=kwargs,
            # pass in the full manifest so we get any local project
            # overrides
            manifest=manifest,
        )

        results = self._catalog_filter_table(table, manifest)
        return results

    def _get_catalog_schemas(self, manifest: Manifest) -> AthenaSchemaSearchMap:
        info_schema_name_map = AthenaSchemaSearchMap()
        nodes: Iterator[CompileResultNode] = chain(
            [
                node
                for node in manifest.nodes.values()
                if (node.is_relational and not node.is_ephemeral_model)
            ],
            manifest.sources.values(),
        )
        for node in nodes:
            relation = self.Relation.create_from(self.config, node)
            info_schema_name_map.add(relation)
        return info_schema_name_map
