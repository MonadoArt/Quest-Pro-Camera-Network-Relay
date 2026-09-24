#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
build="$root/build/host"
mkdir -p "$build"
cmake -S "$root/third-party/libjpeg-turbo" -B "$build/libjpeg-turbo" \
  -DENABLE_SHARED=OFF -DENABLE_STATIC=ON -DWITH_TURBOJPEG=ON \
  -DWITH_SIMD=OFF -DWITH_TOOLS=OFF -DWITH_TESTS=OFF
cmake --build "$build/libjpeg-turbo" --target turbojpeg-static --parallel 4
gcc -std=c11 -O2 -Wall -Wextra -Werror -I "$root/third-party/libjpeg-turbo/src" \
  "$root/daemon/qpro_camd.c" "$build/libjpeg-turbo/libturbojpeg.a" \
  -pthread -lm -o "$build/qpro-camd"
gcc -std=c11 -O2 -Wall -Wextra -Werror "$root/tests/fake_streamer.c" -o "$build/fake-streamer"
python3 - "$build/qpro-camd" "$build/fake-streamer" "$build" <<'PY'
import json
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

daemon, fake, build = sys.argv[1:]
tmp = tempfile.mkdtemp(prefix='qpro-test-', dir=build)
shared = os.path.join(tmp, 'shared.bin')
pidfile = os.path.join(tmp, 'camd.pid')
daemon_log = open(os.path.join(tmp, 'daemon.log'), 'w+')
fake_log = open(os.path.join(tmp, 'fake.log'), 'w+')
port_socket = socket.socket()
port_socket.bind(('127.0.0.1', 0))
port = port_socket.getsockname()[1]
port_socket.close()
base = f'http://127.0.0.1:{port}'
checks = []
children = []

def check(name, good, detail):
    print(f'{"PASS" if good else "FAIL"} {name}: {detail}', flush=True)
    checks.append(bool(good))

def curl(path, seconds=5, rate=None):
    command = ['curl', '-fsSN', '--max-time', str(seconds), base + path]
    if rate:
        command[1:1] = ['--limit-rate', rate]
    return subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

def capture(proc, seconds):
    try:
        return proc.communicate(timeout=seconds + 2)[0]
    except subprocess.TimeoutExpired:
        proc.terminate()
        return proc.communicate(timeout=2)[0]

def frames(data):
    return [image for _, image in parts(data)]

def parts(data):
    result = []
    marker = b'--frame\r\nContent-Type: image/jpeg\r\nContent-Length: '
    pos = 0
    while True:
        start = data.find(marker, pos)
        if start < 0: break
        end = data.find(b'\r\n\r\n', start)
        if end < 0: break
        headers = data[start + len(b'--frame\r\n'):end].decode('ascii', errors='replace')
        fields = {}
        for line in headers.split('\r\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                fields[key.lower()] = value.strip()
        try: size = int(fields['content-length'])
        except ValueError: break
        image = data[end + 4:end + 4 + size]
        if len(image) != size: break
        result.append((fields, image))
        pos = end + 4 + size + 2
    return result

def jpeg_shape(image):
    if not image.startswith(b'\xff\xd8') or not image.endswith(b'\xff\xd9'):
        return None
    pos = 2
    while pos < len(image) - 4:
        if image[pos] != 0xff: return None
        marker = image[pos + 1]
        if marker == 0xff: pos += 1; continue
        if marker in (0xd8, 0xd9) or 0xd0 <= marker <= 0xd7: pos += 2; continue
        size = struct.unpack_from('>H', image, pos + 2)[0]
        if size < 2: return None
        if marker in (0xc0, 0xc1, 0xc2, 0xc3):
            height, width = struct.unpack_from('>HH', image, pos + 5)
            return width, height, image[pos + 9]
        pos += size + 2
    return None

def get_status():
    return json.loads(subprocess.check_output(['curl', '-fsS', base + '/status'], timeout=3))

def lease_value(path=shared):
    with open(path, 'rb') as file:
        file.seek(64)
        return struct.unpack('<Q', file.read(8))[0]

try:
    d = subprocess.Popen([daemon, '--bind', '127.0.0.1', '--port', str(port),
        '--shared-path', shared, '--pid-path', pidfile, '--max-fps', '30'],
        stdout=daemon_log, stderr=subprocess.STDOUT)
    children.append(d)
    version = subprocess.run([daemon, '--version'], capture_output=True, text=True, timeout=3)
    version_ok = version.returncode == 0 and version.stdout.strip() == 'qpro-camd 0.4.0'
    check('version CLI', version_ok, f'exit={version.returncode} output={version.stdout.strip()!r}')
    for _ in range(100):
        if d.poll() is not None: raise RuntimeError(f'daemon exited {d.returncode}')
        if os.path.exists(shared):
            try:
                get_status()
                break
            except (subprocess.CalledProcessError, OSError): pass
        time.sleep(.05)
    else: raise RuntimeError('daemon did not listen')
    idle_status = get_status()
    check('status version and idle source', idle_status.get('version') == '0.4.0' and
        idle_status.get('source', {}).get('state') == 'idle',
        f'version={idle_status.get("version")} source={idle_status.get("source")}')
    f = subprocess.Popen([fake, shared, '30', '--prepublish'], stdout=fake_log, stderr=subprocess.STDOUT)
    children.append(f)
    for _ in range(40):
        if 'PREPUBLISHED' in open(fake_log.name).read(): break
        time.sleep(.05)
    else: raise RuntimeError('fake streamer did not prepublish')

    expected = 400
    start = time.monotonic()
    sample_parts = parts(capture(curl('/camera2.mjpg', 5), 5))
    sample = [image for _, image in sample_parts]
    duration = time.monotonic() - start
    sizes = [len(x) for x in sample]
    shapes = [jpeg_shape(x) for x in sample]
    check('camera2 5s', len(sample) >= 10 and all(x == (expected, 400, 1) for x in shapes),
        f'frames={len(sample)} fps={len(sample)/duration:.1f} jpeg_bytes={min(sizes) if sizes else 0}..{max(sizes) if sizes else 0} SOF={shapes[0] if shapes else None} FFD8/FFD9={all(x.startswith(bytes.fromhex("ffd8")) and x.endswith(bytes.fromhex("ffd9")) for x in sample)}')
    sequences = [int(h['x-sequence']) for h, _ in sample_parts if 'x-sequence' in h]
    check('prepublished frame excluded', bool(sequences) and min(sequences) > 1,
        f'first_sequence={sequences[0] if sequences else None} prepublished_sequence=1')
    check('MJPEG sequence headers', len(sequences) == len(sample_parts) and
        all(b > a for a, b in zip(sequences, sequences[1:])) and
        all('x-timestamp-ns' in h for h, _ in sample_parts),
        f'headers={len(sequences)} frames={len(sample_parts)} increasing={all(b > a for a, b in zip(sequences, sequences[1:]))}')

    a, b = curl('/camera0.mjpg', 3), curl('/camera4.mjpg', 3)
    time.sleep(.3)
    status = get_status()
    check('status active clients', status['clients'][0] >= 1 and status['clients'][4] >= 1 and status['lease_active'] and status['frames_read'] > 0,
        f'clients={status["clients"]} frames_read={status["frames_read"]} lease={status["lease_active"]}')
    check('source ok while publishing', status.get('source', {}).get('state') == 'ok' and
        status.get('source', {}).get('last_frame_age_ms') is not None,
        f'source={status.get("source")}')
    process, source, outputs, device = (status[k] for k in ('process', 'input', 'streams', 'device'))
    sane = (process['cpu_percent'] >= 0 and process['cpu_percent_since_start'] >= 0 and
        process['rss_kb'] > 0 and process['voluntary_context_switches_per_second'] >= 0 and
        process['involuntary_context_switches_per_second'] >= 0 and
        source['frames_read_per_second'] > 0 and source['mean_frame_age_ms'] >= 0 and
        source['max_frame_age_ms'] >= 0 and source['dropped_frames'] >= 0 and
        source['stale_frames_skipped'] >= 0 and len(outputs) == 9 and
        outputs[0]['output_frames_per_second'] > 0 and outputs[4]['output_frames_per_second'] > 0 and
        all(all(k in item and item[k] >= 0 for k in
            ('output_frames_per_second', 'mean_jpeg_bytes', 'bytes_per_second', 'mean_encode_ms')) for item in outputs) and
        all(k in device for k in ('current_now', 'voltage_now', 'capacity', 'temp', 'thermal_zone_temps')))
    check('status telemetry', sane,
        f'cpu={process["cpu_percent"]:.2f}% read_fps={source["frames_read_per_second"]:.2f} camera0_fps={outputs[0]["output_frames_per_second"]:.2f}')
    af, bf = frames(capture(a, 3)), frames(capture(b, 3))
    check('two cameras concurrently', len(af) >= 5 and len(bf) >= 5,
        f'camera0_frames={len(af)} camera4_frames={len(bf)}')

    paused = curl('/camera0.mjpg', 6)
    time.sleep(.25)
    os.kill(f.pid, signal.SIGUSR1)
    time.sleep(2.4)
    paused_status = get_status()
    age1 = paused_status.get('source', {}).get('last_frame_age_ms')
    time.sleep(.3)
    paused_status2 = get_status()
    age2 = paused_status2.get('source', {}).get('last_frame_age_ms')
    check('source waiting while paused', paused_status.get('source', {}).get('state') == 'waiting_for_frames' and
        age1 is not None and age2 is not None and age2 > age1,
        f'state={paused_status.get("source", {}).get("state")} age_ms={age1}->{age2}')
    os.kill(f.pid, signal.SIGUSR1)
    paused.terminate(); paused.communicate(timeout=3)

    slow = curl('/strip.mjpg', 5, '4k')
    time.sleep(.3)
    start = time.monotonic()
    fast_frames = frames(capture(curl('/camera1.mjpg', 5), 5))
    fast_duration = time.monotonic() - start
    slow.terminate()
    slow.communicate(timeout=3)
    check('slow client isolation', len(fast_frames) / fast_duration >= 15,
        f'fast_frames={len(fast_frames)} fast_fps={len(fast_frames)/fast_duration:.1f} slow_rate=4kB/s')

    strip = frames(capture(curl('/strip.mjpg', 2), 2))
    check('strip dimensions', bool(strip) and jpeg_shape(strip[0]) == (2000, 400, 1),
        f'frames={len(strip)} SOF={jpeg_shape(strip[0]) if strip else None}')
    for path, width in (('/eyes.mjpg', 800), ('/face.mjpg', 1200), ('/mouth.mjpg', 800)):
        group_parts = parts(capture(curl(path, 3), 3))
        group_shapes = [jpeg_shape(image) for _, image in group_parts]
        group_sequences = [int(headers['x-sequence']) for headers, _ in group_parts if 'x-sequence' in headers]
        increasing = (len(group_sequences) == len(group_parts) and bool(group_sequences) and
            all(b > a for a, b in zip(group_sequences, group_sequences[1:])))
        check(f'{path[1:-5]} dimensions and sequences',
            bool(group_parts) and all(shape == (width, 400, 1) for shape in group_shapes) and increasing,
            f'frames={len(group_parts)} SOF={group_shapes[0] if group_shapes else None} '
            f'sequences={len(group_sequences)} increasing={increasing}')
    mouth = curl('/mouth.mjpg', 4)
    time.sleep(.3)
    mouth_status = get_status()
    mouth_clients = mouth_status['clients'][8]
    mouth_fps = mouth_status['streams'][8]['output_frames_per_second']
    check('mouth status stream active', mouth_clients >= 1 and mouth_fps > 0,
        f'clients[8]={mouth_clients} output_frames_per_second={mouth_fps}')
    capture(mouth, 4)
    single = subprocess.check_output(['curl', '-fsS', base + '/camera3.jpg'], timeout=3)
    check('single JPEG', jpeg_shape(single) == (400, 400, 1), f'bytes={len(single)} SOF={jpeg_shape(single)}')
    status = get_status()
    check('status JSON', len(status['clients']) == 9 and len(status['mean_encode_ms']) == 9 and status['quality'] == 85,
        f'clients={status["clients"]} mean_encode_ms={status["mean_encode_ms"]}')
    idle_start = time.monotonic()
    def idle_seen():
        transitions = re.findall(r'LEASE_(ACTIVE|IDLE) at=(\d+)', open(fake_log.name).read())
        return bool(transitions) and transitions[-1][0] == 'IDLE' and lease_value() == 0
    while time.monotonic() - idle_start < 3:
        if idle_seen(): break
        time.sleep(.05)
    check('lease idle after clients', idle_seen(),
        f'idle_after={time.monotonic()-idle_start:.2f}s lease={lease_value()}')
    stopped = subprocess.run([daemon, '--stop', '--shared-path', shared, '--pid-path', pidfile],
        capture_output=True, text=True, timeout=5)
    d.wait(timeout=3)
    check('stop clears lease', stopped.returncode == 0 and d.poll() is not None and lease_value() == 0,
        f'stop_exit={stopped.returncode} daemon_exit={d.returncode} lease={lease_value()}')

    # --max-fps only overrides the shared value when passed explicitly.
    coexist = os.path.join(tmp, 'coexist.bin')
    with open(coexist, 'wb') as file:
        file.truncate(80 + 2000 * 400)
        file.seek(72); file.write(struct.pack('<I', 20))
    cfgpid = os.path.join(tmp, 'cfg.pid')
    cfg = subprocess.Popen([daemon, '--bind', '127.0.0.1', '--port', str(port), '--shared-path', coexist, '--pid-path', cfgpid],
        stdout=daemon_log, stderr=subprocess.STDOUT)
    children.append(cfg)
    time.sleep(.4)
    with open(coexist, 'rb') as file: file.seek(72); fps_without = struct.unpack('<I', file.read(4))[0]
    subprocess.run([daemon, '--stop', '--shared-path', coexist, '--pid-path', cfgpid], capture_output=True, timeout=5)
    cfg.wait(timeout=3)
    cfg2 = subprocess.Popen([daemon, '--bind', '127.0.0.1', '--port', str(port), '--shared-path', coexist, '--pid-path', cfgpid, '--max-fps', '30'],
        stdout=daemon_log, stderr=subprocess.STDOUT)
    children.append(cfg2)
    time.sleep(.4)
    with open(coexist, 'rb') as file: file.seek(72); fps_with = struct.unpack('<I', file.read(4))[0]
    subprocess.run([daemon, '--stop', '--shared-path', coexist, '--pid-path', cfgpid], capture_output=True, timeout=5)
    cfg2.wait(timeout=3)
    check('max fps coexistence', fps_without == 20 and fps_with == 30,
        f'without={fps_without} with={fps_with}')

    # --daemonize truncates an existing log.
    trunc_log = os.path.join(tmp, 'truncate.log')
    with open(trunc_log, 'wb') as file: file.write(b'X' * (2 * 1024 * 1024))
    truncpid = os.path.join(tmp, 'trunc.pid')
    truncd = subprocess.Popen([daemon, '--bind', '127.0.0.1', '--port', str(port), '--shared-path', os.path.join(tmp, 'trunc.bin'),
        '--pid-path', truncpid, '--daemonize', '--log', trunc_log], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(.6)
    log_size = os.path.getsize(trunc_log)
    subprocess.run([daemon, '--stop', '--shared-path', os.path.join(tmp, 'trunc.bin'), '--pid-path', truncpid], capture_output=True, timeout=5)
    check('daemon log truncation', log_size < 64 * 1024,
        f'bytes_after_start={log_size} launcher_exit={truncd.returncode}')

    # A longer lease set by another writer survives our client disconnecting.
    leas_shared = os.path.join(tmp, 'lease.bin'); leas_pid = os.path.join(tmp, 'lease.pid')
    ld = subprocess.Popen([daemon, '--bind', '127.0.0.1', '--port', str(port), '--shared-path', leas_shared, '--pid-path', leas_pid],
        stdout=daemon_log, stderr=subprocess.STDOUT)
    children.append(ld)
    for _ in range(100):
        if os.path.exists(leas_shared):
            try: get_status(); break
            except (subprocess.CalledProcessError, OSError): pass
        time.sleep(.05)
    lf = subprocess.Popen([fake, leas_shared, '30'], stdout=fake_log, stderr=subprocess.STDOUT); children.append(lf)
    client = curl('/camera0.mjpg', 4); time.sleep(.5)
    future = time.monotonic_ns() + 60_000_000_000
    with open(leas_shared, 'r+b') as file: file.seek(64); file.write(struct.pack('<Q', future))
    client.terminate(); client.communicate(timeout=3); time.sleep(.5)
    actual_future = lease_value(leas_shared)
    unchanged = actual_future == future
    check('future lease preserved after disconnect', unchanged, f'expected={future} actual={actual_future}')
    subprocess.run([daemon, '--stop', '--shared-path', leas_shared, '--pid-path', leas_pid], capture_output=True, timeout=5)
    ld.wait(timeout=3)
finally:
    for child in children:
        if child.poll() is None: child.terminate()
    for child in children:
        try: child.wait(timeout=3)
        except subprocess.TimeoutExpired: child.kill(); child.wait()
    daemon_log.close(); fake_log.close()
print(f'RESULT {sum(checks)}/{len(checks)} checks passed; logs={tmp}')
sys.exit(0 if checks and all(checks) else 1)
PY
