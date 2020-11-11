#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/t0/alerts/AllConsumingConsumer.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : Unspecified
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import enum
import sys
import uuid

import confluent_kafka

KafkaErrorCode = enum.IntEnum(
    "KafkaErrorCode",
    {
        k: v
        for k, v in confluent_kafka.KafkaError.__dict__.items()
        if isinstance(v, int)
    },
)


class KafkaError(RuntimeError):
    """Picklable wrapper for cimpl.KafkaError"""

    def __init__(self, kafka_err):
        super().__init__(kafka_err.args[0])
        self.code = KafkaErrorCode(kafka_err.code())


class AllConsumingConsumer:
    """
    Consume messages on all topics beginning with 'ztf_'.
    """

    def __init__(self, broker, timeout=None, topics=["^ztf_.*"], **consumer_config):
        """ """

        config = {
            "bootstrap.servers": broker,
            "default.topic.config": {"auto.offset.reset": "smallest"},
            "enable.auto.commit": True,
            "receive.message.max.bytes": 2 ** 29,
            "auto.commit.interval.ms": 10000,
            "enable.auto.offset.store": False,
            "group.id": uuid.uuid1(),
            "enable.partition.eof": False,  # don't emit messages on EOF
            "topic.metadata.refresh.interval.ms": 1000,  # fetch new metadata every second to pick up topics quickly
            # "debug": "all",
        }
        config.update(**consumer_config)
        if "stats_cb" in config and "statistics.interval.ms" not in config:
            config["statistics.interval.ms"] = 60000
        self._consumer = confluent_kafka.Consumer(**config)

        self._consumer.subscribe(topics)
        if timeout is None:
            self._poll_interval = 1
            self._poll_attempts = sys.maxsize
        else:
            self._poll_interval = max((1, min((30, timeout))))
            self._poll_attempts = max((1, int(timeout / self._poll_interval)))
        self._timeout = timeout

        self._last_message = None

    def __del__(self):
        # NB: have to explicitly call close() here to prevent
        # rd_kafka_consumer_close() from segfaulting. See:
        # https://github.com/confluentinc/confluent-kafka-python/issues/358
        self._consumer.close()

    def __next__(self):
        message = self.consume()
        if message is None:
            raise StopIteration
        else:
            return message

    def __iter__(self):
        return self

    def consume(self):
        """
        Block until one message has arrived, and return it.
        
        Messages returned to the caller marked for committal
        upon the _next_ call to consume().
        """
        # mark the last emitted message for committal
        if self._last_message is not None:
            self._consumer.store_offsets(self._last_message)
        self._last_message = None

        message = None
        for _ in range(self._poll_attempts):
            # wake up occasionally to catch SIGINT
            message = self._consumer.poll(self._poll_interval)
            if message is not None:
                if err := message.error():
                    if err.code() == KafkaErrorCode.UNKNOWN_TOPIC_OR_PART:
                        # ignore unknown topic messages
                        continue
                    elif err.code() in (
                        KafkaErrorCode._TIMED_OUT,
                        KafkaErrorCode._MAX_POLL_EXCEEDED,
                    ):
                        # bail on timeouts
                        return None
                break
        else:
            return message

        if message.error():
            raise KafkaError(message.error())
        else:
            self._last_message = message
            return message
