from typing import TypedDict
from app.entities.column_info import ColumnInfo

class DataAgentState(TypedDict):
    error: str  # 校验SQL时出现的错误信息
    query: str  # 用户查询
    keywords: list[str]  # 关键词
    retrieved_column_infos: list[ColumnInfo]  # 检索到的字段信息