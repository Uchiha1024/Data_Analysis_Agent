from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.core.log import logger


async def validate_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    step = "validate sql"
    writer({"type": "progress", "step": step, "status": "running"})

    try:
        sql = state["sql"]
        dw_mysql_repository: DWMySQLRepository = runtime.context["dw_mysql_repository"]

        try:
            await dw_mysql_repository.validate(sql)
            logger.info("SQL syntax is valid")
            writer({"type": "progress", "step": step, "status": "success"})
            return {"error": None}
        except Exception as e:
            logger.info(f"SQL syntax error: {str(e)}")
            writer({"type": "progress", "step": step, "status": "success"})
            return {"error": str(e)}
    except Exception as e:
        logger.error(f"{step} failed: {e}")
        writer({"type": "progress", "step": step, "status": "error"})
        raise


