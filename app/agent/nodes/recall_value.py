from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.agent.llm import llm
from app.prompt.prompt_loader import load_prompt
from app.core.log import logger
from app.entities.value_info import ValueInfo
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from typing import Any

async def recall_value(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("recall value from ES")

    query = state["query"]
    keywords = state["keywords"]
    value_es_repository = runtime.context["value_es_repository"]


    # Expand keywords with LLM
    prompt = PromptTemplate(template=load_prompt("extend_keywords_for_value_recall"),input_variables=["query"])
    output_parser = JsonOutputParser()
    chain = prompt | llm | output_parser
    result = await chain.ainvoke({"query": query})
    keywords = set[Any](keywords + result)

    # Search value info from ES
    value_info_map : dict[str, ValueInfo] = {}
    for keyword in keywords:
        current_value_infos = await value_es_repository.search(keyword)
        for value_info in current_value_infos:
            if value_info.id not in value_info_map:
                value_info_map[value_info.id] = value_info

    # convert value info map to list
    retrieved_value_infos = list[ValueInfo](value_info_map.values())
    # logger.info(f"value recall result: {list[str](value_info_map.keys())}")
    logger.info(f"value recall result: {retrieved_value_infos}")
    return {"retrieved_value_infos": retrieved_value_infos}

