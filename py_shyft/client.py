import asyncio
import traceback

import grpc.aio
from rich.console import Console
import orjson

from py_shyft import REGIONS

from .generated.geyser_pb2 import (
    CommitmentLevel,
    GetBlockHeightRequest,
    GetBlockHeightResponse,
    GetLatestBlockhashRequest,
    GetLatestBlockhashResponse,
    GetSlotRequest,
    GetSlotResponse,
    GetVersionRequest,
    GetVersionResponse,
    IsBlockhashValidRequest,
    IsBlockhashValidResponse,
    PingRequest,
    PongResponse,
    SubscribeRequest,
    SubscribeUpdate,
)
from .generated.geyser_pb2_grpc import GeyserStub
from google.protobuf.json_format import Parse


class ShyftClient:
    def __init__(self, token: str, region: str = "EU"):
        self.console = Console(log_time_format="%Y-%m-%d %H:%M:%S.%f")

        self.token = token
        self.endpoint = REGIONS.get(region)
        if not self.endpoint:
            raise ValueError(f"Invalid region: {region}")

        self.reconnect_delay = 1  # Delay in seconds before attempting to reconnect

    async def __aenter__(self):
        await self.authenticate()
        await self.check_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        if self.channel:
            await self.channel.close()
        self.console.log("Closed all connections")

    async def authenticate(self):
        self.console.log("Attempting to authenticate...")
        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(),
            grpc.metadata_call_credentials(
                lambda context, callback: callback([("x-token", self.token)], None)
            ),
        )
        self.channel = grpc.aio.secure_channel(f"{self.endpoint}:443", credentials)
        self.stub = GeyserStub(self.channel)

    async def check_connection(self):
        pong = await self.ping()
        if pong.count != 1:
            raise RuntimeError(
                "Ping failed, secure connection could not be established"
            )
        self.console.log("Ping successful, secure connection established!")

    async def ping(self, count: int = 1) -> PongResponse:
        try:
            return await self.stub.Ping(PingRequest(count=count))
        except grpc.RpcError as e:
            self.console.log(f"Ping RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    async def get_latest_blockhash(
        self, commitment: CommitmentLevel = CommitmentLevel.PROCESSED
    ) -> GetLatestBlockhashResponse:
        try:
            return await self.stub.GetLatestBlockhash(
                GetLatestBlockhashRequest(commitment=commitment)
            )
        except grpc.RpcError as e:
            self.console.log(f"Blockhash RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    async def get_block_height(
        self, commitment: CommitmentLevel = CommitmentLevel.PROCESSED
    ) -> GetBlockHeightResponse:
        try:
            return await self.stub.GetBlockHeight(
                GetBlockHeightRequest(commitment=commitment)
            )
        except grpc.RpcError as e:
            self.console.log(f"Block height RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    async def get_slot(
        self, commitment: CommitmentLevel = CommitmentLevel.PROCESSED
    ) -> GetSlotResponse:
        try:
            return await self.stub.GetSlot(GetSlotRequest(commitment=commitment))
        except grpc.RpcError as e:
            self.console.log(f"Slot RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    async def is_blockhash_valid(
        self, blockhash: str, commitment: CommitmentLevel = CommitmentLevel.PROCESSED
    ) -> IsBlockhashValidResponse:
        try:
            return await self.stub.IsBlockhashValid(
                IsBlockhashValidRequest(blockhash=blockhash, commitment=commitment)
            )
        except grpc.RpcError as e:
            self.console.log(f"Blockhash validation RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    async def get_version(self) -> GetVersionResponse:
        try:
            return await self.stub.GetVersion(GetVersionRequest())
        except grpc.RpcError as e:
            self.console.log(f"Version RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    async def subscribe(self, filters: dict) -> SubscribeUpdate:
        try:
            request = self.create_subscribe_request(filters)
            return self.stub.Subscribe(iter([request]))
        except grpc.RpcError as e:
            self.console.log(f"Subscribe RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    def create_subscribe_request(self, filters: dict) -> SubscribeRequest:
        try:
            request = Parse(orjson.dumps(filters), SubscribeRequest())
            return request
        except Exception as e:
            raise ValueError("Invalid filters") from e
