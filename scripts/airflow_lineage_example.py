from typing import List

import pandas as pd
from bigquery_sql_parser import query
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.datajob import DataJobInfoClass
from datahub.metadata.schema_classes import ChangeTypeClass, DataFlowInfoClass, DataJobInputOutputClass


def init_emitter(host: str):
    return DatahubRestEmitter(host)


def construct_airflow_dag(dag_id: str, emitter: DatahubRestEmitter) -> str:
    # Construct the DataJobInfo aspect with the job -> flow lineage.
    dataflow_urn = builder.make_data_flow_urn(
        orchestrator="airflow", flow_id=dag_id, cluster="prod"
    )

    dataflow_info = DataFlowInfoClass(name=dag_id)

    dataflow_info_mcp = MetadataChangeProposalWrapper(
        entityType="dataflow",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=dataflow_urn,
        aspectName="dataFlowInfo",
        aspect=dataflow_info,
    )

    emitter.emit_mcp(dataflow_info_mcp)

    return dataflow_urn


def construct_airflow_task(dataflow_urn: str, dag_id: str, task_id: str, emitter: DatahubRestEmitter) -> str:
    datajob_info = DataJobInfoClass(name=task_id, type="AIRFLOW", flowUrn=dataflow_urn)

    datajob_urn = builder.make_data_job_urn(
        orchestrator="airflow", flow_id=dag_id, job_id=task_id, cluster="prod"
    )

    # Construct a MetadataChangeProposalWrapper object with the DataJobInfo aspect.
    # NOTE: This will overwrite all of the existing dataJobInfo aspect information associated with this job.
    datajob_info_mcp = MetadataChangeProposalWrapper(
        entityType="dataJob",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=datajob_urn,
        aspectName="dataJobInfo",
        aspect=datajob_info,
    )

    # Emit metadata!
    emitter.emit_mcp(datajob_info_mcp)

    return datajob_urn


def construct_slave_to_bq_lineage(db_type: str, src_table_name: str, bq_table_name: str,
                                  emitter: DatahubRestEmitter, datajob_urn):
    input_datasets: List[str] = []
    output_datasets: List[str] = []

    input_datasets.append(builder.make_dataset_urn(platform=db_type, name=src_table_name, env="PROD"))
    output_datasets.append(builder.make_dataset_urn(platform="bigquery", name=bq_table_name, env="PROD"))

    datajob_input_output = DataJobInputOutputClass(
        inputDatasets=input_datasets,
        outputDatasets=output_datasets,
    )

    # Construct a MetadataChangeProposalWrapper object.
    datajob_input_output_mcp = MetadataChangeProposalWrapper(
        entityType="dataJob",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=datajob_urn,
        aspectName="dataJobInputOutput",
        aspect=datajob_input_output,
    )

    emitter.emit_mcp(datajob_input_output_mcp)


def construct_bq_to_bq_lineage(src_table_name: List[str], target_table_name: str,
                               emitter: DatahubRestEmitter, datajob_urn):
    input_datasets: List[str] = []
    output_datasets: List[str] = []

    for name in src_table_name:
        input_datasets.append(builder.make_dataset_urn(platform="bigquery", name=name, env="PROD"))

    output_datasets.append(builder.make_dataset_urn(platform="bigquery", name=target_table_name, env="PROD"))

    datajob_input_output = DataJobInputOutputClass(
        inputDatasets=input_datasets,
        outputDatasets=output_datasets,
    )

    # Construct a MetadataChangeProposalWrapper object.
    datajob_input_output_mcp = MetadataChangeProposalWrapper(
        entityType="dataJob",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=datajob_urn,
        aspectName="dataJobInputOutput",
        aspect=datajob_input_output,
    )

    emitter.emit_mcp(datajob_input_output_mcp)


if __name__ == "__main__":
    df = pd.read_csv("bq_to_bq_lineage.csv")
    em = init_emitter("http://localhost:8080")

    # Extract slave-to-bq lineage
    for i in range(len(df)):
        df_urn = construct_airflow_dag(df["dag_name"][i], em)
        job_urn = construct_airflow_task(df_urn, df["dag_name"][i], "{}_task".format(df["source_table_name"][i]), em)

        if df["db_type"][i] == "postgresql":
            construct_slave_to_bq_lineage("postgres",
                                          "{}.public.{}".format(df["db_name"][i], df["source_table_name"][i]),
                                          df["bq_table"][i], em, job_urn)
        else:
            construct_slave_to_bq_lineage(df["db_type"][i],
                                          "{}.{}".format(df["db_name"][i], df["source_table_name"][i]),
                                          df["bq_table"][i], em, job_urn)

    # Extract bq-to-bq lineage
    for i in range(len(df)):
        src_tables: list = []

        df_urn = construct_airflow_dag(df["dag_name"][i], em)
        job_urn = construct_airflow_task(df_urn, df["dag_name"][i], "{}_task".format(df["task_name"][i]), em)

        try:
            src_tables = query.Query(df["query"][i]).full_table_ids
            construct_bq_to_bq_lineage(src_tables, df["target_table"][i], em, job_urn)
        except:
            continue
