from datetime import date
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.state import DBInfoState, DataAgentState, DateInfoState
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.core.log import logger


async def add_extra_context(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    step = "add extra context"
    writer = runtime.stream_writer
    writer({"type": "progress", "step": step, "status": "running"})
    try:
        dw_mysql_repository: DWMySQLRepository = runtime.context["dw_mysql_repository"]

        today = date.today()
        date_str = today.strftime("%Y-%m-%d")
        weekday = today.strftime("%A")
        quarter = f"Q{(today.month - 1) // 3 + 1}"
        date_info = DateInfoState(date=date_str, weekday=weekday, quarter=quarter)

        db = await dw_mysql_repository.get_db_info()
        db_info = DBInfoState(**db)

        writer({"type": "progress", "step": step, "status": "success"})
        logger.info(f"date info: {date_info}")
        logger.info(f"db info: {db_info}")
        return {"date_info": date_info, "db_info": db_info}
    except Exception as e:
        logger.error(f"add extra context failed: {e}")
        writer({"type": "progress", "step": step, "status": "error"})
        raise e
