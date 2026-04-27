from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.core.log import logger


async def validate_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("validate sql")

    sql = state["sql"]

    dw_mysql_repository: DWMySQLRepository = runtime.context["dw_mysql_repository"]
    try:
        await dw_mysql_repository.validate(sql)
        logger.info(f"sql is right")
        return {"error": None}
    except Exception as e:
        logger.error(f"sql is err: {str(e)}")
        return {"error": str(e)}


