from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState, MetricInfoState
from app.prompt.prompt_loader import load_prompt
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from app.agent.llm import llm
from yaml import dump
import yaml
from app.core.log import logger

async def filter_metric(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    step = "filter metric info"
    writer = runtime.stream_writer
    writer({"type": "progress", "step": step, "status": "running"})
    try:
        query = state["query"]
        metric_infos: list[MetricInfoState] = state["metric_infos"]
        prompt = PromptTemplate(
            template=load_prompt("filter_metric_info"),
            input_variables=["query", "metric_infos"],
        )
        output_parser = JsonOutputParser()
        chain = prompt | llm | output_parser
        metric_infos_yaml = yaml.dump(metric_infos, allow_unicode=True, sort_keys=False)
        result = await chain.ainvoke({"query": query, "metric_infos": metric_infos_yaml})
        filtered_metric_infos = [metric_info for metric_info in metric_infos if metric_info["name"] in result]

        writer({"type": "progress", "step": step, "status": "success"})
        logger.info(f"filtered metric infos: {[metric_info['name'] for metric_info in filtered_metric_infos]}")
        return {"metric_infos": filtered_metric_infos}
    except Exception as e:
        logger.error(f"filter metric info failed: {e}")
        writer({"type": "progress", "step": step, "status": "error"})
        raise e


