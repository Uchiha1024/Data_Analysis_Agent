from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState, TableInfoState
from app.agent.llm import llm
from app.prompt.prompt_loader import load_prompt
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from yaml import dump
import yaml
from app.core.log import logger

async def filter_table(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    step = "filter table info"
    writer = runtime.stream_writer
    writer({"type": "progress", "step": step, "status": "running"})
    try:
        query = state["query"]
        table_infos: list[TableInfoState] = state["table_infos"]

        prompt = PromptTemplate(
            template=load_prompt("filter_table_info"),
            input_variables=["query", "table_infos"],
        )
        output_parser = JsonOutputParser()
        chain = prompt | llm | output_parser
        table_infos_yaml = yaml.dump(table_infos, allow_unicode=True, sort_keys=False)

        result = await chain.ainvoke({"query": query, "table_infos": table_infos_yaml})
        filtered_table_infos: list[TableInfoState] = []
        for table_info in table_infos:
            if table_info["name"] in result:
                table_info["columns"] = [
                    column_info
                    for column_info in table_info["columns"]
                    if column_info["name"] in result[table_info["name"]]
                ]
                filtered_table_infos.append(table_info)

        writer({"type": "progress", "step": step, "status": "success"})
        logger.info(f"filtered table infos: {[table_info['name'] for table_info in filtered_table_infos]}")
        return {"table_infos": filtered_table_infos}
    except Exception as e:
        logger.error(f"filter table info failed: {e}")
        writer({"type": "progress", "step": step, "status": "error"})
        raise e
