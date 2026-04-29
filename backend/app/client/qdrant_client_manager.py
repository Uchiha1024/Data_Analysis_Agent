import asyncio
import random
from typing import Optional

from qdrant_client import AsyncQdrantClient, models

from app.conf.app_config import QdrantConfig, app_config


class QdrantClientManager:
    def __init__(self, qdrant_config: QdrantConfig):
        self.qdrant_config = qdrant_config
        self.client: Optional[AsyncQdrantClient] = None

    def _get_url(self):
        # Build the base URL for the Qdrant service.
        return f"http://{self.qdrant_config.host}:{self.qdrant_config.port}"

    def init(self):
        # Initialize the async Qdrant client.
        self.client = AsyncQdrantClient(url=self._get_url())

    async def close(self):
        # Close the underlying async client connection.
        await self.client.close()


# A shared/global client manager instance for the application.
qdrant_client_manager = QdrantClientManager(app_config.qdrant)

if __name__ == '__main__':
    # Initialize client before running the async test routine.
    qdrant_client_manager.init()


    async def test():
        # Basic sanity test: create collection, upsert, then query.
        client = qdrant_client_manager.client
        if not await client.collection_exists("my_collection"):
            await client.create_collection(
                collection_name="my_collection",
                vectors_config=models.VectorParams(size=10, distance=models.Distance.COSINE),
            )

        # Insert sample vectors for retrieval testing.
        await client.upsert(
            collection_name="my_collection",
            points=[
                models.PointStruct(
                    id=i,
                    vector=[random.random() for _ in range(10)],
                )
                for i in range(100)
            ],
        )

        # Run a similarity query and print top results.
        res = await client.query_points(
            collection_name="my_collection",
            query=[random.random() for _ in range(10)],  # type: ignore
            limit=10,
            score_threshold=0.8
        )

        print(res)


        await qdrant_client_manager.close()


    asyncio.run(test())
