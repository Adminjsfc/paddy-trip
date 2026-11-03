#!/usr/bin/env python3
"""Start a fresh loopback InfluxDB for synthetic tests; never use server credentials."""
import json, os, pathlib, secrets, socket, subprocess, sys, tempfile, time, urllib.request

binary = pathlib.Path(sys.argv[1]).resolve()
root = pathlib.Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix='jsfc-native-influx-') as folder:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    base = f'http://127.0.0.1:{port}'
    token = secrets.token_urlsafe(48)
    with open(pathlib.Path(folder) / 'influx.log', 'wb') as logs:
        server = subprocess.Popen([str(binary), '--http-bind-address', f'127.0.0.1:{port}',
            '--bolt-path', str(pathlib.Path(folder) / 'influx.bolt'),
            '--engine-path', str(pathlib.Path(folder) / 'engine'),
            '--sqlite-path', str(pathlib.Path(folder) / 'influx.sqlite'),
            '--reporting-disabled', '--log-level', 'error'], stdout=logs, stderr=logs)
        try:
            for _ in range(100):
                if server.poll() is not None:
                    raise RuntimeError('Local InfluxDB exited during startup')
                try:
                    with urllib.request.urlopen(base + '/health', timeout=1) as response:
                        if response.status == 200: break
                except Exception: time.sleep(0.1)
            else: raise RuntimeError('Local InfluxDB readiness timed out')
            setup = json.dumps({'username':'fixture', 'password':secrets.token_urlsafe(20),
                'org':'jsfc-test','bucket':'gps_fixture','token':token,'retentionPeriodSeconds':0}).encode()
            request = urllib.request.Request(base + '/api/v2/setup', data=setup, headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status not in [200, 201]: raise RuntimeError('Local setup failed')
            env = dict(os.environ, JSFC_TEST_INFLUX_URL=base, JSFC_TEST_INFLUX_TOKEN=token)
            subprocess.run(['node','scripts/validate-influx-index-native.cjs'],cwd=root,env=env,check=True,timeout=90)
        finally:
            server.terminate()
            try: server.wait(timeout=10)
            except subprocess.TimeoutExpired: server.kill(); server.wait()
