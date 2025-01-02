from collections.abc import AsyncGenerator

import backoff
import grpc.aio
import orjson
from google.protobuf.json_format import Parse
from rich.console import Console

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


class GrpcConnectionManager:
    def __init__(self, token: str, endpoint: str):
        self.token = token
        self.endpoint = endpoint
        self.channel = None
        self.stub = None
        self._closed = False

    async def get_stub(self) -> GeyserStub:
        if (
            not self.channel
            or self.channel.get_state() != grpc.ChannelConnectivity.READY
        ):
            await self.connect()
        return self.stub

    @backoff.on_exception(backoff.expo, (grpc.RpcError, ConnectionError), max_time=300)
    async def connect(self):
        if self._closed:
            raise ConnectionError("Connection manager is closed")

        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(),
            grpc.metadata_call_credentials(
                lambda context, callback: callback([("x-token", self.token)], None)
            ),
        )
        self.channel = grpc.aio.secure_channel(f"{self.endpoint}:443", credentials)
        self.stub = GeyserStub(self.channel)

    async def close(self):
        self._closed = True
        if self.channel:
            await self.channel.close()


class ShyftClient:
    def __init__(self, token: str, region: str = "EU"):
        self.console = Console(log_time_format="%Y-%m-%d %H:%M:%S.%f")

        self.token = token
        self.endpoint = REGIONS.get(region)
        if not self.endpoint:
            raise ValueError(f"Invalid region: {region}")

        self.connection_manager = GrpcConnectionManager(self.token, self.endpoint)

    async def __aenter__(self):
        await self.connection_manager.connect()
        await self.check_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.connection_manager.close()
        self.console.log("Closed all connections")

    async def check_connection(self):
        pong = await self.ping()
        if pong.count != 1:
            raise RuntimeError(
                "Ping failed, secure connection could not be established"
            )
        self.console.log("Ping successful, secure connection established!")

    async def ping(self, count: int = 1) -> PongResponse:
        try:
            stub = await self.connection_manager.get_stub()
            return await stub.Ping(PingRequest(count=count))
        except grpc.RpcError as e:
            self.console.log(f"Ping RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    async def get_latest_blockhash(
        self, commitment: CommitmentLevel = CommitmentLevel.PROCESSED
    ) -> GetLatestBlockhashResponse:
        try:
            stub = await self.connection_manager.get_stub()
            return await stub.GetLatestBlockhash(
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
            stub = await self.connection_manager.get_stub()
            return await stub.GetBlockHeight(
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
            stub = await self.connection_manager.get_stub()
            return await stub.GetSlot(GetSlotRequest(commitment=commitment))
        except grpc.RpcError as e:
            self.console.log(f"Slot RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    async def is_blockhash_valid(
        self, blockhash: str, commitment: CommitmentLevel = CommitmentLevel.PROCESSED
    ) -> IsBlockhashValidResponse:
        try:
            stub = await self.connection_manager.get_stub()
            return await stub.IsBlockhashValid(
                IsBlockhashValidRequest(blockhash=blockhash, commitment=commitment)
            )
        except grpc.RpcError as e:
            self.console.log(f"Blockhash validation RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    async def get_version(self) -> GetVersionResponse:
        try:
            stub = await self.connection_manager.get_stub()
            return await stub.GetVersion(GetVersionRequest())
        except grpc.RpcError as e:
            self.console.log(f"Version RPC error: {e.code()}")
            self.console.log(f"Details: {e.details()}")
        except Exception as e:
            self.console.log(f"Unexpected error: {e}")

    async def subscribe(self, filters: dict) -> AsyncGenerator[SubscribeUpdate, None]:
        while True:
            try:
                stub = await self.connection_manager.get_stub()
                request = self.create_subscribe_request(filters)
                async for message in stub.Subscribe(iter([request])):
                    yield message
            except grpc.RpcError as e:
                self.console.log(f"Subscription RPC error: {e.code()}")
                self.console.log(f"Details: {e.details()}")
                continue
            except Exception as e:
                self.console.log(f"Unexpected error: {e}")
                break

    def create_subscribe_request(self, filters: dict) -> SubscribeRequest:
        try:
            request = Parse(orjson.dumps(filters), SubscribeRequest())
            return request
        except Exception as e:
            raise ValueError("Invalid filters") from e
