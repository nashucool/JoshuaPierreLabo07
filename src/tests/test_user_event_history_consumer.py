import json
from types import SimpleNamespace

import consumers.user_event_history_consumer as history_module
from consumers.user_event_history_consumer import UserEventHistoryConsumer
from handlers.handler_registry import HandlerRegistry


class FakeKafkaConsumer:
    def __init__(self, messages):
        self.messages = messages
        self.closed = False

    def __iter__(self):
        return iter(self.messages)

    def close(self):
        self.closed = True


def test_history_consumer_reads_earliest_and_writes_one_json_file(
    tmp_path,
    monkeypatch,
):
    messages = [
        SimpleNamespace(value={"event": "UserCreated", "id": 10}),
        SimpleNamespace(value={"event": "UserDeleted", "id": 10}),
    ]
    fake_consumer = FakeKafkaConsumer(messages)
    constructor_arguments = {}

    def fake_constructor(*args, **kwargs):
        constructor_arguments["args"] = args
        constructor_arguments["kwargs"] = kwargs
        return fake_consumer

    monkeypatch.setattr(history_module, "KafkaConsumer", fake_constructor)

    consumer = UserEventHistoryConsumer(
        bootstrap_servers="kafka:9092",
        topic="user-events",
        group_id="coolriel-group-history-test",
        registry=HandlerRegistry(),
        output_dir=str(tmp_path),
        consumer_timeout_ms=25,
    )

    event_count = consumer.start()

    history = json.loads(
        (tmp_path / "user_event_history.json").read_text(encoding="utf-8")
    )
    assert event_count == 2
    assert history == [message.value for message in messages]
    assert constructor_arguments["args"] == ("user-events",)
    assert constructor_arguments["kwargs"]["group_id"] == (
        "coolriel-group-history-test"
    )
    assert constructor_arguments["kwargs"]["auto_offset_reset"] == "earliest"
    assert constructor_arguments["kwargs"]["consumer_timeout_ms"] == 25
    assert constructor_arguments["kwargs"]["enable_auto_commit"] is False
    assert fake_consumer.closed is True
