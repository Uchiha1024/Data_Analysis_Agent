from typing import Optional

from langchain_huggingface import HuggingFaceEndpointEmbeddings

from app.conf.app_config import EmbeddingConfig, app_config


class EmbeddingClientManager:
    def __init__(self, config: EmbeddingConfig):
        self.client: Optional[HuggingFaceEndpointEmbeddings] = None
        self.config = config

    def _get_url(self):
        # Build the embedding service endpoint URL.
        return f"http://{self.config.host}:{self.config.port}"

    def init(self):
        # Initialize the embedding client with the configured endpoint.
        self.client = HuggingFaceEndpointEmbeddings(model=self._get_url())


# Shared embedding client manager instance for application-wide use.
embedding_client_manager = EmbeddingClientManager(app_config.embedding)
