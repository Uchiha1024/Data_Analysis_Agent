from pathlib import Path
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
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
        if meta_config.tables:
            table_info:list[TableInfoMySQL] = []
            column_info:list[ColumnInfoMySQL] = []
            for table in meta_config.tables:
                pass

                for column in table.columns:
                    pass
