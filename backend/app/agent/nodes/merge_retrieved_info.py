from langgraph.runtime import Runtime
from sqlalchemy import column

from app.agent.context import DataAgentContext
from app.agent.state import ColumnInfoState, DataAgentState, MetricInfoState, TableInfoState
from app.entities.column_info import ColumnInfo
from app.entities.metric_info import MetricInfo
from app.entities.value_info import ValueInfo
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.core.log import logger


async def merge_retrieved_info(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    step = "merge retrieved info"
    writer = runtime.stream_writer
    writer({"type": "progress", "step": step, "status": "running"})
    try:
        retrieved_column_infos: list[ColumnInfo] = state["retrieved_column_infos"]
        retrieved_metric_infos: list[MetricInfo] = state["retrieved_metric_infos"]
        retrieved_value_infos: list[ValueInfo] = state["retrieved_value_infos"]
        meta_mysql_repository: MetaMySQLRepository = runtime.context["meta_mysql_repository"]

        retrieved_column_infos_map: dict[str, ColumnInfo] = {column_info.id: column_info for column_info in retrieved_column_infos}
        for metric_info in retrieved_metric_infos:
            for relevant_column in metric_info.relevant_columns:
                if relevant_column not in retrieved_column_infos_map:
                    column_info = await meta_mysql_repository.get_column_info_by_id(relevant_column)
                    if column_info:
                        retrieved_column_infos_map[relevant_column] = column_info

        for value_info in retrieved_value_infos:
            value = value_info.value
            column_id = value_info.column_id
            if column_id not in retrieved_column_infos_map:
                column_info = await meta_mysql_repository.get_column_info_by_id(column_id)
                if column_info:
                    retrieved_column_infos_map[column_id] = column_info
            if value not in retrieved_column_infos_map[column_id].examples:
                retrieved_column_infos_map[column_id].examples.append(value)

        table_to_columns_map: dict[str, list[ColumnInfo]] = {}
        for column_info in retrieved_column_infos_map.values():
            if column_info.table_id not in table_to_columns_map:
                table_to_columns_map[column_info.table_id] = []
            table_to_columns_map[column_info.table_id].append(column_info)

        for table_id in table_to_columns_map.keys():
            key_columns = await meta_mysql_repository.get_key_columns_by_table_id(table_id)
            column_ids = [column_info.id for column_info in table_to_columns_map[table_id]]
            for key_column in key_columns:
                if key_column.id not in column_ids:
                    table_to_columns_map[table_id].append(key_column)

        table_infos: list[TableInfoState] = []
        for table_id, column_infos in table_to_columns_map.items():
            table_info = await meta_mysql_repository.get_table_info_by_id(table_id)
            if table_info:
                columns = [
                    ColumnInfoState(
                        name=column_info.name,
                        type=column_info.type,
                        role=column_info.role,
                        examples=column_info.examples,
                        description=column_info.description,
                        alias=column_info.alias,
                    ) for column_info in column_infos
                ]
                table_info_state = TableInfoState(
                    name=table_info.name,
                    role=table_info.role,
                    description=table_info.description,
                    columns=columns,
                )
                table_infos.append(table_info_state)

        metric_infos: list[MetricInfoState] = []
        for metric_info in retrieved_metric_infos:
            metric_info_state = MetricInfoState(
                name=metric_info.name,
                description=metric_info.description,
                relevant_columns=metric_info.relevant_columns,
                alias=metric_info.alias,
            )
            metric_infos.append(metric_info_state)

        writer({"type": "progress", "step": step, "status": "success"})
        return {
            "table_infos": table_infos,
            "metric_infos": metric_infos,
        }
    except Exception as e:
        logger.error(f"merge retrieved info failed: {e}")
        writer({"type": "progress", "step": step, "status": "error"})
        raise e


