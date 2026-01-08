import argparse
import json
import logging
import logging.handlers
import os
import queue
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


# -----------------------------
# Constants
# -----------------------------

# Queue configuration
DEFAULT_IN_QUEUE_SIZE = 5000      # RX events waiting for processing
DEFAULT_TX_QUEUE_SIZE = 5000      # Outbound datagrams waiting to send

# Socket configuration
DEFAULT_RECV_BUF_BYTES = 4 * 1024 * 1024  # 4MB receive buffer
DEFAULT_MAX_DATAGRAM_BYTES = 65507        # Max UDP datagram size

# Thread configuration
SOCKET_TIMEOUT_SECONDS = 0.5      # Socket timeout for checking stop_event
THREAD_JOIN_TIMEOUT_SECONDS = 2.0 # Time to wait for threads to exit on shutdown

# Health reporting
HEALTH_REPORT_INTERVAL_SECONDS = 30  # How often to log health metrics


# -----------------------------
# Types
# -----------------------------

@dataclass(frozen=True)
class InboundEvent:
    transport: str                  # "udp" or "tcp" (tcp optional)
    data: bytes
    remote_addr: Tuple[str, int]    # (ip, port)
    local_port: int                 # which local port received this
    recv_time: float                # time.time() seconds


@dataclass(frozen=True)
class OutboundDatagram:
    data: bytes
    dest_addr: Tuple[str, int]      # (ip, port)
    local_port: int                 # TX local port (for logging/correlation)
    msg_id: Optional[str] = None


# -----------------------------
# Metrics (thread-safe counters)
# -----------------------------

class Counters:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.rx_packets = 0
        self.rx_dropped_queue_full = 0
        self.rx_parse_ok = 0
        self.rx_parse_fail = 0
        self.tx_packets = 0
        self.tx_fail = 0

    def inc(self, name: str, amount: int = 1) -> None:
        with self._lock:
            setattr(self, name, getattr(self, name) + amount)

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return {
                "rx_packets": self.rx_packets,
                "rx_dropped_queue_full": self.rx_dropped_queue_full,
                "rx_parse_ok": self.rx_parse_ok,
                "rx_parse_fail": self.rx_parse_fail,
                "tx_packets": self.tx_packets,
                "tx_fail": self.tx_fail,
            }


# -----------------------------
# Logging setup (non-blocking)
# -----------------------------

def setup_logging(
    log_dir: str,
    app_name: str = "udp_middleware",
    level: int = logging.INFO,
    enable_event_log: bool = False,
) -> logging.handlers.QueueListener:
    os.makedirs(log_dir, exist_ok=True)

    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=10000)
    queue_handler = logging.handlers.QueueHandler(log_queue)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # let handlers filter; keeps per-logger tuning flexible
    root.handlers.clear()
    root.addHandler(queue_handler)

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    file_path = os.path.join(log_dir, f"{app_name}.log")
    file_handler = logging.handlers.RotatingFileHandler(
        file_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(fmt)

    handlers = [file_handler, console_handler]

    # Optional Windows Event Log (WARNING+ recommended)
    if enable_event_log and os.name == "nt":
        try:
            ev = logging.handlers.NTEventLogHandler(app_name)
            ev.setLevel(logging.WARNING)
            ev.setFormatter(fmt)
            handlers.append(ev)
        except Exception:
            # Don't fail startup if Event Log isn't available/registered
            pass

    listener = logging.handlers.QueueListener(log_queue, *handlers, respect_handler_level=True)
    listener.start()
    return listener


# -----------------------------
# UDP RX thread
# -----------------------------

class UdpReceiver(threading.Thread):
    def __init__(
        self,
        sock: socket.socket,
        local_port: int,
        out_queue: "queue.Queue[InboundEvent]",
        stop_event: threading.Event,
        counters: Counters,
        logger: logging.Logger,
        recv_buf_bytes: int = DEFAULT_RECV_BUF_BYTES,
        max_datagram_bytes: int = DEFAULT_MAX_DATAGRAM_BYTES,
    ) -> None:
        super().__init__(name="UdpReceiver", daemon=True)
        self.sock = sock
        self.local_port = local_port
        self.out_queue = out_queue
        self.stop_event = stop_event
        self.counters = counters
        self.log = logger
        self.max_datagram_bytes = max_datagram_bytes

        # Help with bursts (best effort; OS may adjust)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buf_bytes)
        except OSError as e:
            self.log.error("Failed to set SO_RCVBUF to %d bytes: %s", recv_buf_bytes, e)

        # Timeout so we can check stop_event regularly
        self.sock.settimeout(SOCKET_TIMEOUT_SECONDS)

    def run(self) -> None:
        self.log.info("UDP RX started on port %d", self.local_port)

        while not self.stop_event.is_set():
            try:
                data, addr = self.sock.recvfrom(self.max_datagram_bytes)
                self.counters.inc("rx_packets", 1)

                evt = InboundEvent(
                    transport="udp",
                    data=data,
                    remote_addr=(addr[0], addr[1]),
                    local_port=self.local_port,
                    recv_time=time.time(),
                )

                # Drop-newest policy when full (RX must never block)
                try:
                    self.out_queue.put_nowait(evt)
                except queue.Full:
                    self.counters.inc("rx_dropped_queue_full", 1)
                    self.log.warning(
                        "RX queue full, dropped packet from=%s:%d local_port=%d size=%d",
                        addr[0], addr[1], self.local_port, len(data)
                    )

            except socket.timeout:
                continue
            except OSError as e:
                # Socket closed during shutdown is expected
                if not self.stop_event.is_set():
                    self.log.exception("UDP RX socket error: %s", e)
                break
            except Exception:
                self.log.exception("Unexpected error in UDP RX loop")
                break

        self.log.info("UDP RX stopped")


# -----------------------------
# UDP TX thread
# -----------------------------

class UdpSender(threading.Thread):
    def __init__(
        self,
        sock: socket.socket,
        local_port: int,
        in_queue: "queue.Queue[OutboundDatagram]",
        stop_event: threading.Event,
        counters: Counters,
        logger: logging.Logger,
    ) -> None:
        super().__init__(name="UdpSender", daemon=True)
        self.sock = sock
        self.local_port = local_port
        self.in_queue = in_queue
        self.stop_event = stop_event
        self.counters = counters
        self.log = logger

        self.sock.settimeout(SOCKET_TIMEOUT_SECONDS)

    def run(self) -> None:
        self.log.info("UDP TX started on port %d", self.local_port)

        while not self.stop_event.is_set():
            try:
                pkt = self.in_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                self.sock.sendto(pkt.data, pkt.dest_addr)
                self.counters.inc("tx_packets", 1)
                self.log.debug(
                    "TX ok local_port=%d dest=%s:%d bytes=%d msg_id=%s",
                    self.local_port, pkt.dest_addr[0], pkt.dest_addr[1], len(pkt.data), pkt.msg_id
                )
            except OSError as e:
                self.counters.inc("tx_fail", 1)
                self.log.warning(
                    "TX failed local_port=%d dest=%s:%d msg_id=%s err=%s",
                    self.local_port, pkt.dest_addr[0], pkt.dest_addr[1], pkt.msg_id, e
                )
            except Exception:
                self.counters.inc("tx_fail", 1)
                self.log.exception("Unexpected TX error")

        self.log.info("UDP TX stopped")


# -----------------------------
# Processor thread (parse/route)
# -----------------------------

class Processor(threading.Thread):
    def __init__(
        self,
        in_queue: "queue.Queue[InboundEvent]",
        tx_queue: "queue.Queue[OutboundDatagram]",
        stop_event: threading.Event,
        counters: Counters,
        logger: logging.Logger,
        tx_dest: Tuple[str, int],
    ) -> None:
        super().__init__(name="Processor", daemon=True)
        self.in_queue = in_queue
        self.tx_queue = tx_queue
        self.stop_event = stop_event
        self.counters = counters
        self.log = logger
        self.tx_dest = tx_dest

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def run(self) -> None:
        self.log.info("Processor started")

        while not self.stop_event.is_set():
            try:
                evt = self.in_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            # Decode + JSON parse (do not do this in RX thread)
            try:
                text = evt.data.decode("utf-8")
                obj = json.loads(text)
                self.counters.inc("rx_parse_ok", 1)
            except Exception as e:
                self.counters.inc("rx_parse_fail", 1)
                # Truncate payload in logs
                # TODO: Sanitize payload snippet to prevent log injection attacks
                # (newlines, control chars, etc. could mess with log parsing)
                snippet = evt.data[:200]
                self.log.warning(
                    "Parse fail local_port=%d from=%s:%d err=%s payload_snip=%r",
                    evt.local_port, evt.remote_addr[0], evt.remote_addr[1], e, snippet
                )
                continue

            # Extract or synthesize a message id (good for dedupe/log correlation)
            # TODO: Implement message deduplication using msg_id
            # UDP can deliver duplicates; consider time-windowed dedup cache
            msg_id = None
            if isinstance(obj, dict):
                msg_id = obj.get("msg_id") or obj.get("id")

            # --- ROUTING LOGIC PLACEHOLDER ---
            # Here you:
            # 1) validate fields
            # 2) decide if you send an outbound UDP message
            # 3) or forward to "other process" (TBD) via another queue/adapter
            # TODO: Currently assumes single fixed TX destination (self.tx_dest)
            # If routing to multiple destinations is needed, implement routing logic here

            # Example: echo-ish ack or normalized envelope to TX
            envelope: Dict[str, Any] = {
                "meta": {
                    "recv_time": self._now_iso(),
                    "transport": evt.transport,
                    "local_port": evt.local_port,
                    "remote": f"{evt.remote_addr[0]}:{evt.remote_addr[1]}",
                },
                "payload": obj,
            }
            out_bytes = (json.dumps(envelope, separators=(",", ":")) + "\n").encode("utf-8")

            out = OutboundDatagram(
                data=out_bytes,
                dest_addr=self.tx_dest,
                local_port=-1,  # set by main when enqueuing if you want
                msg_id=msg_id,
            )

            # TX queue can block a bit; that's okay. If you want drop policy here too,
            # use put_nowait + handle queue.Full.
            self.tx_queue.put(out)

            self.log.debug(
                "Processed ok from=%s:%d rx_port=%d msg_id=%s",
                evt.remote_addr[0], evt.remote_addr[1], evt.local_port, msg_id
            )

        self.log.info("Processor stopped")


# -----------------------------
# Periodic health summary thread
# -----------------------------

class HealthReporter(threading.Thread):
    def __init__(
        self,
        counters: Counters,
        stop_event: threading.Event,
        logger: logging.Logger,
        interval_s: int = HEALTH_REPORT_INTERVAL_SECONDS,
    ) -> None:
        super().__init__(name="HealthReporter", daemon=True)
        self.counters = counters
        self.stop_event = stop_event
        self.log = logger
        self.interval_s = interval_s

    def run(self) -> None:
        while not self.stop_event.is_set():
            time.sleep(self.interval_s)
            snap = self.counters.snapshot()
            self.log.info("Health: %s", snap)


# -----------------------------
# Main wiring
# -----------------------------

def validate_ports(rx_port: int, tx_port: int) -> None:
    if rx_port == tx_port:
        raise ValueError("RX and TX ports must differ")


def make_udp_socket(bind_ip: str, port: int) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((bind_ip, port))
    return s


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bind-ip", default="0.0.0.0", help="Local interface IP to bind (default all)")
    ap.add_argument("--rx-port", type=int, default=10000)
    ap.add_argument("--tx-port", type=int, default=10001)
    ap.add_argument("--tx-dest-ip", required=True)
    ap.add_argument("--tx-dest-port", type=int, required=True)
    ap.add_argument("--log-dir", default=os.path.join(os.getcwd(), "logs"))
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    ap.add_argument("--eventlog", action="store_true", help="Enable Windows Event Log (WARNING+)")
    ap.add_argument("--in-queue-size", type=int, default=DEFAULT_IN_QUEUE_SIZE)
    ap.add_argument("--tx-queue-size", type=int, default=DEFAULT_TX_QUEUE_SIZE)
    args = ap.parse_args()

    validate_ports(args.rx_port, args.tx_port)

    level = getattr(logging, args.log_level)
    listener = setup_logging(
        log_dir=args.log_dir,
        app_name="udp_middleware",
        level=level,
        enable_event_log=args.eventlog,
    )

    log = logging.getLogger("app")
    log.info("Starting. bind_ip=%s rx_port=%d tx_port=%d tx_dest=%s:%d",
             args.bind_ip, args.rx_port, args.tx_port, args.tx_dest_ip, args.tx_dest_port)

    stop_event = threading.Event()
    counters = Counters()

    in_q: queue.Queue[InboundEvent] = queue.Queue(maxsize=args.in_queue_size)
    tx_q: queue.Queue[OutboundDatagram] = queue.Queue(maxsize=args.tx_queue_size)

    # Bind sockets
    try:
        rx_sock = make_udp_socket(args.bind_ip, args.rx_port)
        tx_sock = make_udp_socket(args.bind_ip, args.tx_port)  # fixed source port
    except OSError as e:
        log.critical("Failed to bind UDP ports: %s", e, exc_info=True)
        listener.stop()
        return 2

    rx = UdpReceiver(
        sock=rx_sock,
        local_port=args.rx_port,
        out_queue=in_q,
        stop_event=stop_event,
        counters=counters,
        logger=logging.getLogger("ingress.udp"),
    )

    tx = UdpSender(
        sock=tx_sock,
        local_port=args.tx_port,
        in_queue=tx_q,
        stop_event=stop_event,
        counters=counters,
        logger=logging.getLogger("egress.udp"),
    )

    proc = Processor(
        in_queue=in_q,
        tx_queue=tx_q,
        stop_event=stop_event,
        counters=counters,
        logger=logging.getLogger("router"),
        tx_dest=(args.tx_dest_ip, args.tx_dest_port),
    )

    health = HealthReporter(
        counters=counters,
        stop_event=stop_event,
        logger=logging.getLogger("health"),
        interval_s=HEALTH_REPORT_INTERVAL_SECONDS,
    )

    rx.start()
    tx.start()
    proc.start()
    health.start()

    # TODO: Add thread health monitoring - detect and restart dead threads
    # Currently if any worker thread dies unexpectedly, it won't be restarted
    # Consider: periodic aliveness checks, automatic restart, alerting
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        log.info("Shutdown requested (Ctrl+C)")
    finally:
        stop_event.set()
        try:
            rx_sock.close()
        except Exception:
            pass
        try:
            tx_sock.close()
        except Exception:
            pass

        # Give threads a moment to exit
        # Note: timeout may need adjustment if queues are large
        rx.join(timeout=THREAD_JOIN_TIMEOUT_SECONDS)
        proc.join(timeout=THREAD_JOIN_TIMEOUT_SECONDS)
        tx.join(timeout=THREAD_JOIN_TIMEOUT_SECONDS)
        health.join(timeout=THREAD_JOIN_TIMEOUT_SECONDS)

        log.info("Stopped. Final counters=%s", counters.snapshot())
        listener.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
