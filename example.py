import asyncio
import uvloop
from py_shyft.client import ShyftClient

from py_shyft.generated.geyser_pb2 import CommitmentLevel

X_TOKEN = "some-api-token"

FILTERS = {
    "accounts": {
        "usdc_filter": {
            "owner": ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
            "filters": [
                {
                    "memcmp": {
                        "offset": 0,
                        "base58": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                    }
                },
            ],
        }
    },
    "slots": {
        "only_processed_filter": {
            "filter_by_commitment": True,
        }
    },
    "commitment": CommitmentLevel.PROCESSED,
}


async def main():
    async with ShyftClient(X_TOKEN) as client:
        stream = await client.subscribe(FILTERS)
        async for update in stream:
            print(update)


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
