from pathlib import Path
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.entities.table_info import TableInfo
from app.entities.column_info import ColumnInfo
from app.models.table_info import TableInfoMySQL
from app.models.column_info import ColumnInfoMySQL
from omegaconf import OmegaConf
from app.conf.meta_config import MetaConfig


class MetaKnowledgeService:
    def __init__(
        self,
        meta_mysql_repository: MetaMySQLRepository,
        dw_mysql_repository: DWMySQLRepository,
    ):
        self.meta_mysql_repository = meta_mysql_repository
        self.dw_mysql_repository = dw_mysql_repository

    async def build(self, config_path: Path):
        context = OmegaConf.load(config_path)
        schema = OmegaConf.structured(MetaConfig)
        meta_config: MetaConfig = OmegaConf.to_object(OmegaConf.merge(schema, context))
        table_infos: list[TableInfo] = []
        column_infos: list[ColumnInfo] = []

        if meta_config.tables:

            for table in meta_config.tables:
                table_info = TableInfo(
                    id=table.name,
                    name=table.name,
                    role=table.role,
                    description=table.description,
                )
                table_infos.append(table_info)
                # query column_type
                column_type = await self.dw_mysql_repository.get_column_types(
                    table.name
                )

                for column in table.columns:
                    # query column_values
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
            return True


