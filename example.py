import asyncio
import uvloop
from py_shyft.client import ShyftClient
from py_shyft.logging_config import setup_logging

X_TOKEN = "some-api-token"

FILTERS = {
    "accounts": {
        "raydium": {
            "owner": ["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"],
            "filters": [
                {
                    "memcmp": {
                        "offset": 560,  # serum market address position
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
    "commitment": 0,
}


async def main():
    logger = setup_logging(__name__)
    async with ShyftClient(X_TOKEN) as client:
        c = 1
        stream = await client.subscribe(FILTERS)
        async for update in stream:
            print(update)
            if c == 3:
                break
            c += 1


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
