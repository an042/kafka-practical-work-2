import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone


@dataclass
class Event:
    id: int
    text: str
    source: str
    created_at: str  # ISO 8601

    @staticmethod
    def new(id: int) -> "Event":
        return Event(
            id=id,
            text=f"message-{id}",
            source="producer",
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def serialize(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @staticmethod
    def deserialize(data: bytes) -> "Event":
        d = json.loads(data.decode("utf-8"))
        return Event(**d)
