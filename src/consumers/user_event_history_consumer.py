"""
Kafka Historical User Event Consumer (Event Sourcing)
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import json
from pathlib import Path
from logger import Logger
from typing import Optional
from kafka import KafkaConsumer
from handlers.handler_registry import HandlerRegistry

class UserEventHistoryConsumer:
    """A consumer that starts reading Kafka events from the earliest point from a given topic"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        registry: HandlerRegistry,
        output_dir: str = "output",
        consumer_timeout_ms: int = 5000,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.registry = registry
        self.output_dir = Path(output_dir)
        self.consumer_timeout_ms = consumer_timeout_ms
        self.auto_offset_reset = "earliest"
        self.consumer: Optional[KafkaConsumer] = None
        self.logger = Logger.get_instance("UserEventHistoryConsumer")
    
    def start(self) -> int:
        """Start consuming messages from Kafka"""
        self.logger.info(f"Démarrer un consommateur : {self.group_id}")

        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                consumer_timeout_ms=self.consumer_timeout_ms,
                value_deserializer=lambda message: json.loads(
                    message.decode("utf-8")
                ),
                enable_auto_commit=False,
            )

            events = [message.value for message in self.consumer]
            self._write_history(events)
            self.logger.info(
                f"Historique enregistré : {len(events)} événement(s)"
            )
            return len(events)
        except Exception as e:
            self.logger.error(f"Erreur: {e}", exc_info=True)
            raise
        finally:
            self.stop()

    def _write_history(self, events: list[dict]) -> None:
        """Write all consumed events to one JSON file in a single I/O operation."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        history_file = self.output_dir / "user_event_history.json"
        serialized_events = json.dumps(events, ensure_ascii=False, indent=2)
        history_file.write_text(serialized_events, encoding="utf-8")

    def stop(self) -> None:
        """Stop the consumer gracefully"""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Arrêter le consommateur!")
