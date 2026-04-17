from dataclasses import asdict
from pathlib import Path
import uuid

from langchain_huggingface import HuggingFaceEndpointEmbeddings
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.entities.table_info import TableInfo
from app.entities.column_info import ColumnInfo
from app.entities.value_info import ValueInfo
from app.models.table_info import TableInfoMySQL
from app.models.column_info import ColumnInfoMySQL
from app.core.log import logger
from omegaconf import OmegaConf
from app.conf.meta_config import MetaConfig


class MetaKnowledgeService:
    def __init__(
        self,
        meta_mysql_repository: MetaMySQLRepository,
        dw_mysql_repository: DWMySQLRepository,
        column_qdrant_repository: ColumnQdrantRepository,
        embedding_client: HuggingFaceEndpointEmbeddings,
    ):
        self.meta_mysql_repository = meta_mysql_repository
        self.dw_mysql_repository = dw_mysql_repository
        self.column_qdrant_repository = column_qdrant_repository
        self.embedding_client = embedding_client

    async def build(self, config_path: Path):
        print(f"Building meta knowledge from {config_path}")
        context = OmegaConf.load(config_path)
        schema = OmegaConf.structured(MetaConfig)
        meta_config: MetaConfig = OmegaConf.to_object(OmegaConf.merge(schema, context))
        if meta_config.tables:

            column_infos = await self._save_tables_to_meta_db(meta_config)
            logger.info(f"Saved column infos to meta db")
            await self._save_column_infos_to_qdrant(column_infos)
            logger.info(f"Saved column infos to qdrant")
            # save value infos to es
            await self._save_values_to_es(meta_config)

    async def _save_values_to_es(self, meta_config: MetaConfig) -> None:
        await self.value_es_repository.ensure_index()

        value_infos: list[ValueInfo] = []
        for table in meta_config.tables:
            for column in table.columns:
                if column.sync:
                    # query column values from dw mysql
                    current_column_values = (
                        await self.dw_mysql_repository.get_all_column_values(
                            table.name, column.name
                        )
                    )
                    current_value_infos = [
                        ValueInfo(
                            id=f"{table.name}.{column.name}.{current_column_value}",
                            value=current_column_value,
                            column_id=f"{table.name}.{column.name}",
                        )
                        for current_column_value in current_column_values
                    ]

                    value_infos.extend(current_value_infos)
        await self.value_es_repository.index(value_infos)

    async def _save_tables_to_meta_db(
        self, meta_config: MetaConfig
    ) -> list[ColumnInfo]:
        table_infos: list[TableInfo] = []
        column_infos: list[ColumnInfo] = []

        for table in meta_config.tables:
            table_info = TableInfo(
                id=table.name,
                name=table.name,
                role=table.role,
                description=table.description,
            )
            table_infos.append(table_info)
            column_type = await self.dw_mysql_repository.get_column_types(table.name)

            for column in table.columns:
                column_values = await self.dw_mysql_repository.get_column_values(
                    table.name, column.name
                )
                column_info = ColumnInfo(
                    id=f"{table.name}.{column.name}",
                    name=column.name,
                    type=column_type[column.name],
                    role=column.role,
                    examples=column_values,
                    description=column.description,
                    alias=column.alias,
                    table_id=table.name,
                )
                column_infos.append(column_info)

        async with self.meta_mysql_repository.session.begin():
            self.meta_mysql_repository.save_table_infos(table_infos)
            self.meta_mysql_repository.save_column_infos(column_infos)

        return column_infos

    async def _save_column_infos_to_qdrant(
        self, column_infos: list[ColumnInfo]
    ) -> None:
        await self.column_qdrant_repository.ensure_colletion()
        points: list[dict] = []
        for column_info in column_infos:
            points.append(
                {
                    "id": uuid.uuid4(),
                    "embedding_text": column_info.name,
                    "payload": asdict(column_info),
                }
            )
            points.append(
                {
                    "id": uuid.uuid4(),
                    "embedding_text": column_info.description,
                    "payload": asdict(column_info),
                }
            )
            for alia in column_info.alias:
                points.append(
                    {
                        "id": uuid.uuid4(),
                        "embedding_text": alia,
                        "payload": asdict(column_info),
                    }
                )

        embeddings: list[list[float]] = []
        embedding_texts = [point["embedding_text"] for point in points]
        embedding_bath_size = 20
        for i in range(0, len(embedding_texts), embedding_bath_size):
            embedding_texts_batch = embedding_texts[i : i + embedding_bath_size]
            embeddings_batch = await self.embedding_client.aembed_documents(
                embedding_texts_batch
            )
            embeddings.extend(embeddings_batch)

        ids = [point["id"] for point in points]
        payloads = [point["payload"] for point in points]
        await self.column_qdrant_repository.upsert(ids, embeddings, payloads)
