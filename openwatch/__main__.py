import argparse
import json
import math
import tempfile
import sys
from pathlib import Path
from .engine import Engine
from .adapters import JsonFileSource, ConsoleSink


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return number


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def _main():
    parser = argparse.ArgumentParser(description="OpenWatch snapshot monitor")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo", help="replay bundled snapshots in a temporary database")
    scan = commands.add_parser("scan", help="scan one authoritative JSON snapshot")
    scan.add_argument("snapshot")
    scan.add_argument("--source", required=True)
    scan.add_argument("--db", required=True)
    watch = commands.add_parser("watch", help="monitor HTTP JSON in the foreground")
    watch.add_argument("url")
    watch.add_argument("--source", required=True)
    watch.add_argument("--db", required=True)
    watch.add_argument("--interval", type=positive_float, default=60)
    watch.add_argument("--timeout", type=positive_float, default=10)
    watch.add_argument("--max-bytes", type=positive_int, default=1048576)
    watch.add_argument("--cycles", type=positive_int, help="stop after this many scan attempts")
    args = parser.parse_args()
    if args.command == "demo":
        with tempfile.TemporaryDirectory(prefix="openwatch-") as temp:
            engine = Engine(str(Path(temp) / "state.db"))
            try:
                from .engine import Item, Snapshot
                class Replay:
                    def __init__(self, value):
                        self.value = value
                    def scan(self):
                        return Snapshot((Item("release", {"version": self.value}),))
                for value in ("1.0", "1.1", "1.1"):
                    count = engine.scan("demo", Replay(value), ("console",))
                    print(json.dumps({"events_created": count}))
                print(json.dumps(engine.dispatch({"console": ConsoleSink()}, now=0)))
            finally:
                engine.close()
    elif args.command == "watch":
        from .http import HttpJsonSource
        from .runner import Runner
        try:
            source = HttpJsonSource(args.url, timeout=args.timeout, max_bytes=args.max_bytes)
        except ValueError:
            parser.error("invalid HTTP(S) source URL")
        engine = Engine(args.db)
        try:
            runner = Runner(engine, args.source, source, {"console": ConsoleSink()},
                            interval=args.interval)
            runner.run(cycles=args.cycles or 0,
                       report=lambda value: print(json.dumps(value), flush=True))
            if runner.scan_errors or runner.delivery_errors:
                raise SystemExit(1)
        except KeyboardInterrupt:
            print(json.dumps({"stopped": True}), flush=True)
            raise SystemExit(130) from None
        finally:
            if engine.pending_acknowledgements:
                print(json.dumps({"warning": "accepted_delivery_acknowledgements_pending",
                                  "pending_acknowledgements": engine.pending_acknowledgements,
                                  "restart_may_duplicate": True}), file=sys.stderr, flush=True)
            engine.close()
    else:
        engine = Engine(args.db)
        try:
            print(json.dumps({"events_created": engine.scan(
                args.source, JsonFileSource(args.snapshot))}))
        finally:
            engine.close()


def main():
    try:
        return _main()
    except KeyboardInterrupt:
        print(json.dumps({"stopped": True}), flush=True)
        raise SystemExit(130) from None
    except Exception as error:
        # Exception text may contain input payloads, URLs or private file paths.
        print(json.dumps({"error": type(error).__name__}), file=sys.stderr, flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
