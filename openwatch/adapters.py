"""Offline adapters. No credentials, network requests, or browser profiles."""
import json
from pathlib import Path
from .engine import Item, Snapshot, validate_snapshot


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError("non-finite JSON numbers are not supported")


def parse_snapshot(text: str) -> Snapshot:
    try:
        data = json.loads(text, object_pairs_hook=unique_object,
                          parse_constant=reject_constant)
    except (TypeError, RecursionError, UnicodeError):
        raise ValueError("invalid JSON input or unsupported depth") from None
    if not isinstance(data, dict) or type(data.get("complete")) is not bool:
        raise ValueError("snapshot requires explicit boolean complete")
    if not isinstance(data.get("items"), list):
        raise ValueError("snapshot requires items array")
    items = []
    for item in data["items"]:
        if not isinstance(item, dict) or "key" not in item or "value" not in item:
            raise ValueError("each item requires key and value")
        items.append(Item(item["key"], item["value"]))
    snapshot = Snapshot(tuple(items), data["complete"])
    validate_snapshot(snapshot)
    return snapshot


class JsonFileSource:
    """Each file is an authoritative complete snapshot for one source scope."""

    def __init__(self, path: str):
        self.path = Path(path)

    def scan(self) -> Snapshot:
        return parse_snapshot(self.path.read_text(encoding="utf-8"))


class ConsoleSink:
    def send(self, event: dict) -> None:
        print(json.dumps(event, ensure_ascii=False, sort_keys=True), flush=True)
