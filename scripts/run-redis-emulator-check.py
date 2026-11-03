#!/usr/bin/env python3
# Optional test dependency: python3 -m pip install 'fakeredis[lua]'
# Exercises the actual Node/ioredis methods and Lua scripts against an emulator.
import os
import sys
import subprocess
import threading
from pathlib import Path
from fakeredis import TcpFakeServer
# Older emulator releases need RESP2 serialization for ioredis. New releases
# negotiate the protocol correctly and do not need this compatibility patch.
try:
    import fakeredis._tcp_server as tcp
except ModuleNotFoundError:
    tcp = None
if tcp is not None and hasattr(tcp, 'Resp2Writer'):
    _original_dump = tcp.Resp2Writer.dump
    def resp2_dump(self, value, dump_bulk=False):
        if isinstance(value, dict):
            value = [item for pair in value.items() for item in pair]
        return _original_dump(self, value, dump_bulk)
    tcp.Resp2Writer.dump = resp2_dump
    tcp.Resp3Writer = tcp.Resp2Writer
server = TcpFakeServer(('127.0.0.1', 0), server_type='redis')
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
try:
    env = dict(os.environ, REDIS_URL=f'redis://127.0.0.1:{server.server_address[1]}/0')
    root = Path(__file__).resolve().parents[1]
    for script in (sys.argv[1:] or ['scripts/check-redis-queue.cjs', 'scripts/simulate-offline.cjs', 'scripts/simulate-live.cjs', 'scripts/simulate-start-fix.cjs', 'scripts/simulate-independent-trips.cjs', 'scripts/simulate-archive-recovery.cjs']):
        subprocess.run(['node', script], cwd=root, env=env, check=True, timeout=60)
finally:
    server.shutdown()
    server.server_close()
