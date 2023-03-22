import time
import pytest
import asyncio
import aioredis
import subprocess
from .utility import *


class RedisServer:
    def __init__(self):
        self.port = 5555
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen(["redis-server-6.2.11",
                                      f"--port {self.port}",
                                      "--save ''",
                                      "--appendonly no",
                                      "--protected-mode no",
                                      "--repl-diskless-sync yes",
                                      "--repl-diskless-sync-delay 0"])
        print(self.proc.args)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except Exception as e:
            pass

@pytest.fixture(scope="function")
def redis_server() -> RedisServer:
    s = RedisServer()
    s.start()
    time.sleep(1)
    yield s
    s.stop()

@pytest.mark.skipif()
@pytest.mark.asyncio
async def test_resp3(df_local_factory, df_seeder_factory, redis_server):
    r = redis_server

    rc = aioredis.Redis(port=r.port)

    d = df_local_factory.create(port=r.port + 1)
    d.start()

    dc = aioredis.Redis(port=d.port)

    hello = await rc.execute_command("HELLO", "3")
    assert hello[b"proto"] == 3

    hello = await dc.execute_command("HELLO", "3")
    assert hello[b"proto"] == 3

    seeder_config = dict(keys=1000, dbcount=1, unsupported_types=[ValueType.JSON], allow_non_deterministic_actions=False)
    seeder = df_seeder_factory.create(port=[r.port, d.port], **seeder_config)
    await seeder.run(target_deviation=0.1)

    capture = await seeder.capture(port=r.port)
    assert await seeder.compare(capture, port=d.port)
