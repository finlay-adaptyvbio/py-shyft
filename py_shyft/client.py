import asyncio
import os
import random
import traceback

import base58
import grpc.aio
import uvloop
from rich.console import Console


from py_shyft.generated.geyser_pb2_grpc import GeyserAsyncStub
from py_shyft.generated import geyser_pb2


SHYFT_URL = "grpc.fra.shyft.to"
X_TOKEN = "some-api-token"
CONSOLE_WIDTH = 120


class ShyftClient:
    def __init__(self):
        self.console = Console(
            width=CONSOLE_WIDTH, log_time_format="%Y-%m-%d %H:%M:%S.%f"
        )

        self.stub = None
        self.channel = None

        self.reconnect_delay = 1  # Delay in seconds before attempting to reconnect
        self.containers = int(os.environ.get("CONTAINERS", 1))

    async def __aenter__(self):
        await self.create_grpc_channel()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        if self.channel:
            await self.channel.close()
        self.console.log("Closed all connections")

    async def create_grpc_channel(self):
        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(),
            grpc.metadata_call_credentials(
                lambda context, callback: callback([("x-token", X_TOKEN)], None)
            ),
        )
        self.channel = grpc.aio.secure_channel(f"{SHYFT_URL}:443", credentials)
        self.stub = GeyserAsyncStub(self.channel)
        self.console.log("Connected to Shyft gRPC server")

    async def subscribe_to_updates(self, request):
        while True:
            try:
                async for response in self.stub.Subscribe(iter([request])):
                    if response.HasField("slot"):
                        await self.process_slot_update(response.slot)
                    elif response.HasField("account"):
                        await self.process_account_update(response.account)
                    elif response.HasField("transaction"):
                        await self.process_transaction_update(response.transaction)
            except grpc.RpcError as e:
                self.console.log(f"Subscribe RPC error: {e.code()}")
                self.console.log(f"Details: {e.details()}")
                await asyncio.sleep(
                    self.reconnect_delay
                )  # Wait before attempting to reconnect
            except Exception as e:
                self.console.log(f"Unexpected error in subscribe_to_updates: {e}")
                await asyncio.sleep(
                    self.reconnect_delay
                )  # Wait before attempting to reconnect

    async def fetch_latest_blockhash(self):
        while True:
            try:
                blockhash_response = await self.stub.GetLatestBlockhash(
                    geyser_pb2.GetLatestBlockhashRequest()
                )
                await self.process_blockhash_update(blockhash_response)
                await asyncio.sleep(
                    10
                )  # Wait for 10 seconds before fetching the next blockhash
            except grpc.RpcError as e:
                self.console.log(f"Blockhash RPC error: {e.code()}")
                self.console.log(f"Details: {e.details()}")
                await asyncio.sleep(
                    self.reconnect_delay
                )  # Wait before attempting to reconnect
            except Exception as e:
                self.console.log(f"Unexpected error in fetch_latest_blockhash: {e}")
                await asyncio.sleep(
                    self.reconnect_delay
                )  # Wait before attempting to reconnect

    def create_subscribe_request(self):
        request = geyser_pb2.SubscribeRequest()
        request.commitment = geyser_pb2.CommitmentLevel.PROCESSED
        request.slots["all_slots"].filter_by_commitment = True

        accounts_to_monitor = (
            self.markets["pool_1"].values.tolist()
            + self.markets["pool_2"].values.tolist()
        )
        account_filter = request.accounts["account_subscription"]
        for account in accounts_to_monitor:
            account_filter.account.append(account)

        return request

    async def run(self):
        while True:
            try:
                request = self.create_subscribe_request()

                subscription_task = asyncio.create_task(
                    self.subscribe_to_updates(request)
                )
                blockhash_task = asyncio.create_task(self.fetch_latest_blockhash())

                await asyncio.gather(subscription_task, blockhash_task)
            except grpc.RpcError as e:
                self.console.log(f"gRPC error: {e.code()}")
                self.console.log(f"Details: {e.details()}")
            except TypeError as e:
                self.console.log(f"TypeError: {e}")
                self.console.log(f"Full traceback: {traceback.format_exc()}")
            except Exception as e:
                self.console.log(f"Unexpected error: {type(e).__name__}: {e}")
                self.console.log(f"Full traceback: {traceback.format_exc()}")

            self.console.log("Waiting 5 seconds before attempting to reconnect...")
            await asyncio.sleep(5)  # Wait before attempting to reconnect


async def main():
    async with ShyftClient() as client:
        await client.run()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
