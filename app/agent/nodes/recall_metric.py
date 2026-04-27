from typing import Any
from langgraph.runtime import Runtime
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.agent.llm import llm
from app.prompt.prompt_loader import load_prompt
from app.core.log import logger
from app.entities.metric_info import MetricInfo


async def recall_metric(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("recall metric")

    query = state["query"]
    keywords = state["keywords"]
    embedding_client = runtime.context["embedding_client"]
    metric_qdrant_repository = runtime.context["metric_qdrant_repository"]

    # read prompt from file
    prompt = PromptTemplate(template=load_prompt("extend_keywords_for_metric_recall"),input_variables=["query"])
    output_parser = JsonOutputParser()
    # build chain
    chain = prompt | llm | output_parser

    result = await chain.ainvoke({"query": query})
    keywords = set[Any](keywords + result)
    # search metric info from qdrant
    metric_info_map : dict[str, MetricInfo] = {}
    for keyword in keywords:
        # embed keyword
        embedding = await embedding_client.aembed_query(keyword)
        current_metric_infos = await metric_qdrant_repository.search(embedding)
        for metric_info in current_metric_infos:
            if metric_info.id not in metric_info_map:
                metric_info_map[metric_info.id] = metric_info

    # convert metric info map to list
    retrieved_metric_infos = list[MetricInfo](metric_info_map.values())
    logger.info(f"metric recall result: {retrieved_metric_infos}")
    # logger.info(f"metric recall result: {list[str](metric_info_map.keys())}")
    return {"retrieved_metric_infos": retrieved_metric_infos}



