# generated by datamodel-codegen:
#   filename:  catalog_latest.json

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import ConfigDict, Field

from dbt_artifacts_parser.parsers.base import BaseParserModel


class Metadata(BaseParserModel):
    model_config = ConfigDict(
        extra='allow',
    )
    dbt_schema_version: Optional[str] = None
    dbt_version: Optional[str] = '1.9.0b2'
    generated_at: Optional[str] = None
    invocation_id: Optional[str] = None
    env: Optional[Dict[str, str]] = None


class Metadata1(BaseParserModel):
    model_config = ConfigDict(
        extra='allow',
    )
    type: str
    schema_: str = Field(..., alias='schema')
    name: str
    database: Optional[str] = None
    comment: Optional[str] = None
    owner: Optional[str] = None


class Columns(BaseParserModel):
    model_config = ConfigDict(
        extra='allow',
    )
    type: str
    index: int
    name: str
    comment: Optional[str] = None


class Stats(BaseParserModel):
    model_config = ConfigDict(
        extra='allow',
    )
    id: str
    label: str
    value: Optional[Union[bool, str, float]] = None
    include: bool
    description: Optional[str] = None


class Nodes(BaseParserModel):
    model_config = ConfigDict(
        extra='allow',
    )
    metadata: Metadata1 = Field(..., title='TableMetadata')
    columns: Dict[str, Columns]
    stats: Dict[str, Stats]
    unique_id: Optional[str] = None


class Sources(BaseParserModel):
    model_config = ConfigDict(
        extra='allow',
    )
    metadata: Metadata1 = Field(..., title='TableMetadata')
    columns: Dict[str, Columns]
    stats: Dict[str, Stats]
    unique_id: Optional[str] = None


class CatalogLatest(BaseParserModel):
    model_config = ConfigDict(
        extra='allow',
    )
    metadata: Metadata = Field(..., title='CatalogMetadata')
    nodes: Dict[str, Nodes]
    sources: Dict[str, Sources]
    errors: Optional[List[str]] = None
    field_compile_results: Any = Field(None, alias='_compile_results')
