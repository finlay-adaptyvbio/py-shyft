import asyncio
import uvloop
from py_shyft.client import ShyftClient

from py_shyft.generated.geyser_pb2 import CommitmentLevel

X_TOKEN = "some-api-token"

FILTERS = {
    "accounts": {
        "raydium": {
            "owner": ["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"],
            "filters": [
                {
                    "memcmp": {
                        "offset": 560,
                        "base58": "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",
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
        async for update in client.subscribe(FILTERS):
            print(update)


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
