import ipaddress
import threading
import argparse
import logging
import socket
import struct
import time
import queue
import zlib
import sys
import re

import pyaudio


DISCOVERY_PACKET_MAGIC = b'AstR'
DISCOVERY_PACKET_VERSION = 2
DISCOVERY_PACKET_FMT_H = '!4sH'
DISCOVERY_PACKET_FMT_D = '4sHHIBB'
DISCOVERY_PACKET_FIELDS = ('addr', 'port', 'framerate', 'rate', 'bits', 'channels')

DEFAULT_DISCOVERY_PORT = 32123
DEFAULT_MULTICAST_ADDR = '239.32.12.3'
DEFAULT_MULTICAST_PORT = 32124

DEFAULT_FRAMERATE = 100


root_logger = logger = logging.getLogger()
logging.basicConfig(level=logging.ERROR)


class DiscoverySenderThread(threading.Thread):
    def __init__(self, stop_event, framerate, rate, bits, channels, discovery_port=None, multicast_addr=None, multicast_port=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_event = stop_event
        self.discovery_port = discovery_port or DEFAULT_DISCOVERY_PORT
        self.packet = struct.pack(DISCOVERY_PACKET_FMT_H, DISCOVERY_PACKET_MAGIC, DISCOVERY_PACKET_VERSION) \
            + struct.pack(DISCOVERY_PACKET_FMT_D, ipaddress.ip_address(multicast_addr or DEFAULT_MULTICAST_ADDR).packed,
                multicast_port or DEFAULT_MULTICAST_PORT, framerate, rate, bits, channels)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def run(self):
        while not self.stop_event.is_set():
            self.sock.sendto(self.packet, ('<broadcast>', self.discovery_port))
            t = time.time()
            while not self.stop_event.is_set() and time.time() - t < 3:
                time.sleep(0.25)
        self.sock.close()


def receive_discovery_packet(port=None, timeout=10):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) # UDP
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(timeout)
    sock.bind(("", DEFAULT_DISCOVERY_PORT))
    try:
        t_start = time.time()
        while time.time() - t_start < timeout:
            try:
                data, addr = sock.recvfrom(1024)
                if data[:4] != DISCOVERY_PACKET_MAGIC:
                    continue
                magic, version = struct.unpack(DISCOVERY_PACKET_FMT_H, data[:struct.calcsize(DISCOVERY_PACKET_FMT_H)])
                if version != DISCOVERY_PACKET_VERSION:
                    continue
                out = dict(
                    zip(DISCOVERY_PACKET_FIELDS, struct.unpack(DISCOVERY_PACKET_FMT_D, data[struct.calcsize(DISCOVERY_PACKET_FMT_H):])),
                    version=version,
                    from_addr=addr,
                )
                out['addr'] = str(ipaddress.ip_address(out['addr']))
                logger.info("Got discovery packet from %s - receive data on %s:%d at %d KHz/%d bits/%d channels, %d FPS", out['from_addr'][0], out['addr'], out['port'], out['rate'], out['bits'], out['channels'], out['framerate'])
                return out
            except socket.timeout:
                pass
    finally:
        sock.close()


class AudioCaptureThread(threading.Thread):
    def __init__(self, stop_event, output_queue, device_index_name, framerate, rate, bits, channels, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_event = stop_event
        self.output_queue = output_queue
        self.framerate = framerate
        self.audio = pyaudio.PyAudio()
        self.stream = self.audio.open(
            format=getattr(pyaudio, 'paInt' + str(bits)),
            channels=channels,
            rate=rate,
            input=True,
            frames_per_buffer=int(rate / framerate),
            input_device_index=self.get_device_index(device_index_name),
            stream_callback=self.handle_data
        )

    def get_device_index(self, index_or_name):
        devices = {}
        for i in range(self.audio.get_device_count()):
            info = self.audio.get_device_info_by_index(i)
            if info.get('maxInputChannels') > 0:
                devices[i] = info.get('name')

        try:
            index_or_name = int(index_or_name)
            if index_or_name in devices:
                return index_or_name
        except (ValueError, TypeError):
            pass

        index_or_name = str(index_or_name)
        for i, name in devices.items():
            if name == index_or_name:
                return i

            if re.match(index_or_name, name):
                return i

        raise ValueError("Invalid device index or name '{}' - valid devices are {}".format(
            index_or_name,
            ', '.join((str(k) + ': ' + v for k, v in devices.items()))
        ))

    def run(self):
        try:
            self.stream.start_stream()
            while not self.stop_event.is_set() and self.stream.is_active():
                time.sleep(1 / self.framerate)
        finally:
            self.stream.stop_stream()
            self.stream.close()
            self.audio.terminate()

    def handle_data(self, in_data, frame_count, time_info, status_flags):
        self.output_queue.put(in_data)
        return (None, pyaudio.paContinue)


class CompressionThread(threading.Thread):
    def __init__(self, stop_event, input_queue, output_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_event = stop_event
        self.input_queue = input_queue
        self.output_queue = output_queue

    def run(self):
        while not self.stop_event.is_set():
            try:
                data = self.input_queue.get(timeout=0.1)
                data = zlib.compress(data, level=4)
                data = struct.pack('!H', len(data)) + data
                self.output_queue.put(data)
            except queue.Empty:
                pass


class DecompressionThread(threading.Thread):
    def __init__(self, stop_event, input_queue, output_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_event = stop_event
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.buffer = b''

    def run(self):
        while not self.stop_event.is_set():
            while self.buffer and not self.stop_event.is_set():
                data_len, = struct.unpack('!H', self.buffer[:2])
                if len(self.buffer) - 2 <= data_len:
                    break
                data = self.buffer[2:data_len + 2]
                data = zlib.decompress(data)
                self.output_queue.put(data)
                self.buffer = self.buffer[data_len + 2:]

            try:
                while True:
                    self.buffer += self.input_queue.get(block=False)
            except queue.Empty:
                pass


class MulticastSendThread(threading.Thread):
    def __init__(self, stop_event, input_queue, multicast_addr=None, multicast_port=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_event = stop_event
        self.input_queue = input_queue
        self.multicast_group = (multicast_addr or DEFAULT_MULTICAST_ADDR, multicast_port or DEFAULT_MULTICAST_PORT)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setblocking(0)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack('b', 1))

    def run(self):
        while not self.stop_event.is_set():
            try:
                data = self.input_queue.get(timeout=0.1)
                self.sock.sendto(data, self.multicast_group)
            except queue.Empty:
                pass
        self.sock.close()


class MulticastReceiveThread(threading.Thread):
    def __init__(self, stop_event, output_queue, multicast_addr, multicast_port, framerate, rate, bits, channels, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_event = stop_event
        self.output_queue = output_queue
        self.packet_size = int((((bits / 8) * channels) * rate) / framerate) + 2
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(1)
        self.sock.bind(('', multicast_port))
        group = socket.inet_aton(multicast_addr)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    def run(self):
        while not self.stop_event.is_set():
            try:
                data, addr = self.sock.recvfrom(self.packet_size * 2)
                self.output_queue.put(data)
            except socket.timeout:
                pass


class AudioPlaybackThread(threading.Thread):
    def __init__(self, stop_event, input_queue, rate, bits, channels, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_event = stop_event
        self.input_queue = input_queue
        self.frame_size = int((bits / 8) * channels)
        self.buffer = b''
        self.audio = pyaudio.PyAudio()
        self.stream = self.audio.open(
            format=getattr(pyaudio, 'paInt' + str(bits)),
            channels=channels,
            rate=rate,
            output=True,
            stream_callback=self.handle_data
        )

    def run(self):
        try:
            self.stream.start_stream()
            while not self.stop_event.is_set() and self.stream.is_active():
                time.sleep(0.1)
        finally:
            self.stream.stop_stream()
            self.stream.close()
            self.audio.terminate()

    def handle_data(self, in_data, frame_count, time_info, status):
        need_bytes = frame_count * self.frame_size
        while len(self.buffer) < need_bytes and not self.stop_event.is_set():
            try:
                self.buffer += self.input_queue.get(block=False)
            except queue.Empty:
                pass

        out = self.buffer[:need_bytes]
        self.buffer = self.buffer[need_bytes:]
        return (out, pyaudio.paContinue)


def parse_args():
    main_parser = argparse.ArgumentParser(description="Transmit audio over the network", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    main_parser.add_argument('--debug', action='store_true', help="Turn on debug logging")
    subparsers = main_parser.add_subparsers()

    capture_parser = subparsers.add_parser('capture', help="Capture and transmit audio", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    capture_parser.add_argument('device', help="Capture audio from this device.  Can be a device index, name, or regular expression")
    capture_parser.add_argument('--rate', '-r', default=48000, help="Sample rate in KHz")
    capture_parser.add_argument('--bits', '-b', default=32, help="Sample bit rate")
    capture_parser.add_argument('--channels', '-c', default=2, help="Number of channels to capture")
    capture_parser.add_argument('--framerate', '-f', default=DEFAULT_FRAMERATE, help="Frame rate (packets per second)")
    capture_parser.add_argument('--discovery-port', '-d', default=DEFAULT_DISCOVERY_PORT, help="Broadcast discovery packets on this port")
    capture_parser.add_argument('--multicast-addr', '-a', default=DEFAULT_MULTICAST_ADDR, help="Send audio data to this multicast address")
    capture_parser.add_argument('--multicast-port', '-p', default=DEFAULT_MULTICAST_PORT, help="Send audio data on this multicast port")
    capture_parser.set_defaults(cmd='capture')

    receive_parser = subparsers.add_parser('receive', help="Receive audio data over the network and play it back", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    receive_parser.add_argument('--discovery-port', '-d', default=DEFAULT_DISCOVERY_PORT, help="Listen for discovery packets on this port")
    receive_parser.set_defaults(cmd='receive')

    return main_parser.parse_args()


def main(args):
    if args.debug:
        root_logger.setLevel(logging.DEBUG)

    threads = []
    stop_event = threading.Event()
    raw_queue = queue.Queue()
    comp_queue = queue.Queue()

    if args.cmd == 'capture':
        logger.info("Preparing threads for capture from device '%s' at %d KHz/%d bit/%d channels, %d FPS", args.device, args.rate, args.bits, args.channels, args.framerate)
        logger.info("Will send multicast data to %s:%s, broadcast discovery packets on port %d", args.multicast_addr, args.multicast_port, args.discovery_port)
        threads = [
            DiscoverySenderThread(stop_event, args.framerate, args.rate, args.bits, args.channels, discovery_port=args.discovery_port, multicast_addr=args.multicast_addr, multicast_port=args.multicast_port),
            AudioCaptureThread(stop_event, raw_queue, args.device, args.framerate, args.rate, args.bits, args.channels),
            CompressionThread(stop_event, raw_queue, comp_queue),
            MulticastSendThread(stop_event, comp_queue, multicast_addr=args.multicast_addr, multicast_port=args.multicast_port),
        ]
    elif args.cmd == 'receive':
        logger.debug("Waiting for discovery packet on port %d...", args.discovery_port)
        data = receive_discovery_packet(port=args.discovery_port)
        if not data:
            logger.error("Did not receive discovery packet")
            return -1
        threads = [
            MulticastReceiveThread(stop_event, comp_queue, data['addr'], data['port'], data['framerate'], data['rate'], data['bits'], data['channels']),
            DecompressionThread(stop_event, comp_queue, raw_queue),
            AudioPlaybackThread(stop_event, raw_queue, data['rate'], data['bits'], data['channels']),
        ]

    if not threads:
        logger.critical("No threads to run")
        return -98

    try:
        logger.info("Start %d threads", len(threads))
        for t in threads:
            t.start()

        while True:
            time.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("Exiting - SIGINT")
    finally:
        logger.info("Stopping threads")
        stop_event.set()
        for t in threads:
            t.join()


if __name__ == '__main__':
    try:
        sys.exit(main(parse_args()) or 0)
    except SystemExit:
        pass
    except:
        logger.critical("Exception in main thread", exc_info=True)
        sys.exit(-99)
