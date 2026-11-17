import asyncio
import threading
import time
import typing as tp

import pytest

import pypeln as pl
from pypeln.task.api.from_kafka import KafkaMessage, OffsetTracker, TopicPartition

TP0 = TopicPartition("t", 0)
TP1 = TopicPartition("t", 1)


# ----------------------------------------------------------------
# helpers
# ----------------------------------------------------------------


class EndOfData(Exception):
    pass


class CommitError(Exception):
    pass


class FakeRecord(tp.NamedTuple):
    topic: str
    partition: int
    offset: int
    value: tp.Any
    key: tp.Any = None
    headers: tp.Any = None
    timestamp: tp.Any = None


class FakeConsumer:
    """
    In-memory implementation of the `from_kafka` consumer protocol.
    `committed_store` plays the role of the broker's committed offsets and
    can be shared by several consumers to simulate restarts.
    """

    def __init__(
        self,
        data: tp.Dict[tp.Tuple[str, int], tp.List[tp.Tuple[int, tp.Any]]],
        committed_store: dict,
        batch_size: int = 1,
        end_event: tp.Optional[threading.Event] = None,
        fail_commit: bool = False,
    ):
        self.data = {
            TopicPartition(topic, partition): sorted(records)
            for (topic, partition), records in data.items()
        }
        self.committed_store = committed_store
        self.batch_size = batch_size
        self.end_event = end_event
        self.fail_commit = fail_commit

        self.assignment: tp.List[TopicPartition] = []
        self.cursors: tp.Dict[TopicPartition, int] = {}
        self.seek_calls = []
        self.commit_calls = []
        self.started = False
        self.closed = False

    async def start(self):
        self.started = True

    def assign(self, partitions):
        self.assignment = list(partitions)
        self.cursors = {tp: 0 for tp in self.assignment}

    def partitions_for_topic(self, topic):
        return sorted(tp.partition for tp in self.data if tp.topic == topic)

    async def committed(self, tp):
        return self.committed_store.get(tp)

    def seek(self, tp, offset):
        self.seek_calls.append((tp, offset))

        records = self.data[tp]
        cursor = 0

        while cursor < len(records) and records[cursor][0] < offset:
            cursor += 1

        self.cursors[tp] = cursor

    def exhausted(self):
        return all(self.cursors[tp] >= len(self.data[tp]) for tp in self.assignment)

    async def poll(self, timeout):
        deadline = time.monotonic() + timeout

        while True:
            batch = []

            for tp in self.assignment:
                records = self.data[tp]
                cursor = self.cursors[tp]

                while cursor < len(records) and len(batch) < self.batch_size:
                    offset, value = records[cursor]
                    batch.append(FakeRecord(tp.topic, tp.partition, offset, value))
                    cursor += 1

                self.cursors[tp] = cursor

                if len(batch) >= self.batch_size:
                    break

            if batch:
                return batch

            if (
                self.exhausted()
                and self.end_event is not None
                and self.end_event.is_set()
            ):
                raise EndOfData()

            if time.monotonic() >= deadline:
                return []

            await asyncio.sleep(0.005)

    async def commit(self, offsets):
        self.commit_calls.append(dict(offsets))

        if self.fail_commit:
            raise CommitError("failed to commit offsets")

        self.committed_store.update(offsets)

    async def close(self):
        self.closed = True


def wait_until(predicate, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if predicate():
            return True

        time.sleep(0.01)

    return False


def run_pipeline(stage):
    errors = []

    def target():
        try:
            pl.task.run(stage)
        except BaseException as e:
            errors.append(e)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()

    return thread, errors


def finish_pipeline(thread, errors, end_event, expect=EndOfData):
    end_event.set()
    thread.join(timeout=15)

    assert not thread.is_alive(), "pipeline did not terminate"
    assert any(
        isinstance(error, expect) for error in errors
    ), f"expected {expect}, got: {errors}"


# ----------------------------------------------------------------
# OffsetTracker
# ----------------------------------------------------------------


def test_tracker_contiguous_prefix():
    tracker = OffsetTracker()

    for offset in range(5):
        tracker.deliver(TP0, offset)

    # no acks yet: the frontier just restates the start position
    assert tracker.pending_commits() == {}

    assert tracker.ack(tracker.generation, TP0, 4)
    assert tracker.ack(tracker.generation, TP0, 2)
    assert tracker.pending_commits() == {}

    assert tracker.ack(tracker.generation, TP0, 0)
    assert tracker.pending_commits() == {TP0: 1}

    tracker.mark_committed({TP0: 1})
    assert tracker.pending_commits() == {}

    # offset 1 is still pending: later acks cannot move the prefix past it
    assert tracker.ack(tracker.generation, TP0, 3)
    assert tracker.pending_commits() == {}

    assert tracker.ack(tracker.generation, TP0, 1)
    assert tracker.pending_commits() == {TP0: 5}


def test_tracker_offset_holes_are_not_pending():
    tracker = OffsetTracker()

    for offset in [0, 2, 3, 7]:
        tracker.deliver(TP0, offset)

    tracker.ack(tracker.generation, TP0, 0)
    assert tracker.pending_commits() == {TP0: 2}

    tracker.ack(tracker.generation, TP0, 3)
    assert tracker.pending_commits() == {TP0: 2}

    tracker.ack(tracker.generation, TP0, 2)
    assert tracker.pending_commits() == {TP0: 7}

    tracker.ack(tracker.generation, TP0, 7)
    assert tracker.pending_commits() == {TP0: 8}


def test_tracker_partitions_advance_independently():
    tracker = OffsetTracker()

    tracker.deliver(TP0, 0)
    tracker.deliver(TP1, 0)

    tracker.ack(tracker.generation, TP0, 0)
    assert tracker.pending_commits() == {TP0: 1}

    tracker.mark_committed({TP0: 1})

    tracker.ack(tracker.generation, TP1, 0)
    assert tracker.pending_commits() == {TP1: 1}


def test_tracker_ignores_stale_generations_and_closed_tracker():
    tracker = OffsetTracker()
    tracker.deliver(TP0, 0)

    # ack stamped with another run's generation
    assert not tracker.ack(tracker.generation + 1, TP0, 0)
    assert tracker.pending_commits() == {}

    assert tracker.ack(tracker.generation, TP0, 0)
    assert tracker.pending_commits() == {TP0: 1}

    tracker.close()
    tracker.deliver(TP0, 1)

    assert not tracker.ack(tracker.generation, TP0, 1)


def test_tracker_ignores_duplicate_and_unknown_acks():
    tracker = OffsetTracker()
    tracker.deliver(TP0, 0)

    assert tracker.ack(tracker.generation, TP0, 0)
    assert not tracker.ack(tracker.generation, TP0, 0)
    assert not tracker.ack(tracker.generation, TP0, 7)
    assert not tracker.ack(tracker.generation, TP1, 0)


def test_tracker_respects_initial_committed():
    tracker = OffsetTracker()
    tracker.set_committed(TP0, 5)

    tracker.deliver(TP0, 5)
    tracker.deliver(TP0, 6)

    assert tracker.pending_commits() == {}

    tracker.ack(tracker.generation, TP0, 5)
    assert tracker.pending_commits() == {TP0: 6}

    tracker.mark_committed({TP0: 6})
    tracker.ack(tracker.generation, TP0, 6)
    assert tracker.pending_commits() == {TP0: 7}


# ----------------------------------------------------------------
# KafkaMessage
# ----------------------------------------------------------------


def test_kafka_message_ack_is_idempotent_and_loop_bound():
    loop = asyncio.new_event_loop()
    calls = []

    message = KafkaMessage(
        record=FakeRecord("t", 0, 5, "x"),
        ack_fn=lambda *args: calls.append(args),
        generation=3,
        loop=loop,
    )

    assert message.value == "x"
    assert not message.acked

    assert message.ack()
    assert message.acked
    assert not message.ack()  # duplicate acks are dropped

    loop.run_until_complete(asyncio.sleep(0.01))
    assert calls == [(TP0, 5, 3)]

    loop.close()

    late_message = KafkaMessage(
        record=FakeRecord("t", 0, 6, "y"),
        ack_fn=lambda *args: calls.append(args),
        generation=3,
        loop=loop,
    )

    # the run's loop is gone: the late ack is ignored instead of raising
    assert not late_message.ack()
    assert calls == [(TP0, 5, 3)]


# ----------------------------------------------------------------
# from_kafka
# ----------------------------------------------------------------


def test_from_kafka_validation():
    with pytest.raises(ValueError):
        pl.task.from_kafka(lambda: None)

    with pytest.raises(ValueError):
        pl.task.from_kafka(lambda: None, topics=["t"], max_in_flight=0)

    with pytest.raises(ValueError):
        pl.task.from_kafka(lambda: None, topics=["t"], commit_interval=0)

    with pytest.raises(ValueError):
        pl.task.from_kafka(lambda: None, topics=["t"], poll_timeout=-1)

    with pytest.raises(TypeError):
        pl.task.from_kafka("not-a-callable", topics=["t"])


def test_from_kafka_map_each():
    store = {}
    end_event = threading.Event()
    data = {
        ("t", 0): [(0, "a"), (1, "b"), (2, "c")],
        ("t", 1): [(0, "x"), (1, "y")],
    }
    consumer = FakeConsumer(data, store, batch_size=2, end_event=end_event)

    stage = pl.task.from_kafka(
        lambda: consumer, topics=["t"], max_in_flight=16, commit_interval=0.05
    )

    def clean(message):
        message.ack()
        return message.value.upper()

    results = []
    stage = stage | pl.task.map(clean, workers=4)
    stage = pl.task.each(results.append, stage, workers=1)

    thread, errors = run_pipeline(stage)

    assert wait_until(lambda: len(results) == 5)

    # the consumer is exhausted, so no new messages arrive; commits must
    # still complete, driven by the acks alone
    assert consumer.exhausted()
    assert wait_until(
        lambda: store == {TP0: 3, TP1: 2}
    ), f"commits did not complete: {store}"

    finish_pipeline(thread, errors, end_event)

    assert sorted(results) == ["A", "B", "C", "X", "Y"]
    assert store == {TP0: 3, TP1: 2}
    assert consumer.started
    assert consumer.closed


def test_out_of_order_acks_do_not_skip_pending_records():
    store = {}
    end_event = threading.Event()
    data = {("t", 0): [(i, f"v{i}") for i in range(5)]}
    consumer = FakeConsumer(data, store, batch_size=5, end_event=end_event)

    stage = pl.task.from_kafka(
        lambda: consumer, partitions=[("t", 0)], commit_interval=0.05
    )

    received = []

    def collect(message):
        received.append(message)
        return message.value

    stage = pl.task.map(collect, stage, workers=1)

    thread, errors = run_pipeline(stage)

    assert wait_until(lambda: len(received) == 5)
    received.sort(key=lambda message: message.offset)

    received[4].ack()
    received[2].ack()
    time.sleep(0.3)

    # offsets 2 and 4 are acked, but offset 0 is still pending
    assert store == {}

    received[0].ack()
    assert wait_until(lambda: store.get(TP0) == 1)

    received[3].ack()
    time.sleep(0.3)

    # offset 1 is still pending: the prefix cannot move past it
    assert store == {TP0: 1}

    received[1].ack()
    assert wait_until(lambda: store.get(TP0) == 5)

    finish_pipeline(thread, errors, end_event)
    assert store == {TP0: 5}


def test_offset_holes_are_not_treated_as_pending():
    store = {}
    end_event = threading.Event()
    # offsets 1, 4, 5 and 6 are never delivered (e.g. transaction markers)
    data = {("t", 0): [(0, "a"), (2, "b"), (3, "c"), (7, "d")]}
    consumer = FakeConsumer(data, store, batch_size=4, end_event=end_event)

    stage = pl.task.from_kafka(
        lambda: consumer, partitions=[("t", 0)], commit_interval=0.05
    )

    received = []

    def collect(message):
        received.append(message)
        return message.value

    stage = pl.task.map(collect, stage, workers=1)

    thread, errors = run_pipeline(stage)

    assert wait_until(lambda: len(received) == 4)
    received.sort(key=lambda message: message.offset)

    received[0].ack()  # offset 0
    assert wait_until(lambda: store.get(TP0) == 2)

    received[2].ack()  # offset 3, while offset 2 is pending
    time.sleep(0.3)
    assert store == {TP0: 2}

    received[1].ack()  # offset 2
    assert wait_until(lambda: store.get(TP0) == 7)

    received[3].ack()  # offset 7
    assert wait_until(lambda: store.get(TP0) == 8)

    finish_pipeline(thread, errors, end_event)
    assert store == {TP0: 8}


def test_partitions_advance_independently():
    store = {}
    end_event = threading.Event()
    data = {
        ("t", 0): [(0, "a"), (1, "b")],
        ("t", 1): [(0, "x"), (1, "y"), (2, "z")],
    }
    consumer = FakeConsumer(data, store, batch_size=1, end_event=end_event)

    stage = pl.task.from_kafka(lambda: consumer, topics=["t"], commit_interval=0.05)

    received = []

    def collect(message):
        received.append(message)
        return message.value

    stage = pl.task.map(collect, stage, workers=2)

    thread, errors = run_pipeline(stage)

    assert wait_until(lambda: len(received) == 5)

    for message in received:
        if message.partition == 0:
            message.ack()

    assert wait_until(lambda: store.get(TP0) == 2)
    time.sleep(0.3)

    # partition 1 has no acks, so it must not move at all
    assert TP1 not in store

    for message in received:
        if message.partition == 1:
            message.ack()

    assert wait_until(lambda: store.get(TP1) == 3)

    finish_pipeline(thread, errors, end_event)
    assert store == {TP0: 2, TP1: 3}


def test_backpressure_still_processes_acks_and_commits():
    store = {}
    end_event = threading.Event()
    gate = threading.Event()
    data = {("t", 0): [(i, f"v{i}") for i in range(5)]}
    consumer = FakeConsumer(data, store, batch_size=1, end_event=end_event)

    stage = pl.task.from_kafka(
        lambda: consumer,
        partitions=[("t", 0)],
        max_in_flight=16,
        commit_interval=0.05,
    )

    processed = []

    async def slow(message):
        message.ack()
        processed.append(message.offset)

        while not gate.is_set():
            await asyncio.sleep(0.01)

        return message.value

    stage = pl.task.map(slow, stage, workers=1, maxsize=1)

    thread, errors = run_pipeline(stage)

    assert wait_until(lambda: len(processed) == 1)
    time.sleep(0.2)  # let the pipeline settle into the backpressured state

    # the poller is blocked by the full downstream queues, but the ack of
    # the record being processed is still committed
    assert wait_until(lambda: store.get(TP0) == 1)
    assert processed == [0]

    gate.set()

    assert wait_until(lambda: store.get(TP0) == 5)
    finish_pipeline(thread, errors, end_event)
    assert store == {TP0: 5}


def test_in_flight_uncommitted_is_bounded():
    store = {}
    end_event = threading.Event()
    gate = threading.Event()
    data = {("t", 0): [(i, f"v{i}") for i in range(10)]}
    consumer = FakeConsumer(data, store, batch_size=1, end_event=end_event)

    stage = pl.task.from_kafka(
        lambda: consumer,
        partitions=[("t", 0)],
        max_in_flight=3,
        commit_interval=0.05,
    )

    received = []

    async def blocking(message):
        while not gate.is_set():
            await asyncio.sleep(0.01)

        message.ack()
        received.append(message.offset)

        return message.value

    stage = pl.task.map(blocking, stage, workers=1)

    thread, errors = run_pipeline(stage)

    # 3 permits are held by the delivered records; one more record may be
    # prefetched by the poller, but consumption must stop there
    assert wait_until(lambda: consumer.cursors.get(TP0, 0) >= 4)
    time.sleep(0.3)
    assert consumer.cursors[TP0] == 4
    assert store == {}

    gate.set()

    assert wait_until(lambda: store.get(TP0) == 10)
    finish_pipeline(thread, errors, end_event)
    assert received == list(range(10))


def test_commit_failure_is_visible():
    store = {}
    data = {("t", 0): [(i, f"v{i}") for i in range(3)]}
    consumer = FakeConsumer(data, store, batch_size=1, fail_commit=True)

    stage = pl.task.from_kafka(
        lambda: consumer, partitions=[("t", 0)], commit_interval=0.05
    )

    def clean(message):
        message.ack()
        return message.value

    stage = pl.task.map(clean, stage, workers=1)

    thread, errors = run_pipeline(stage)

    assert wait_until(lambda: len(errors) > 0)

    thread.join(timeout=15)

    assert not thread.is_alive()
    assert isinstance(errors[0], CommitError)
    assert consumer.closed


def test_restart_from_committed_and_late_acks_are_ignored():
    store = {}
    data = {("t", 0): [(i, f"v{i}") for i in range(5)]}

    # ---------------- run 1: everything acked except offset 2 ----------------
    end_event_1 = threading.Event()
    consumer_1 = FakeConsumer(data, store, batch_size=2, end_event=end_event_1)

    stage = pl.task.from_kafka(
        lambda: consumer_1, partitions=[("t", 0)], commit_interval=0.05
    )

    processed_1 = []
    saved = []

    def process_1(message):
        processed_1.append(message.offset)

        if message.offset == 2:
            saved.append(message)  # left unacknowledged
        else:
            message.ack()

        return message.value

    stage = pl.task.map(process_1, stage, workers=1)

    thread_1, errors_1 = run_pipeline(stage)

    assert wait_until(lambda: processed_1 == [0, 1, 2, 3, 4])

    # offsets 3 and 4 are acked but cannot be committed: offset 2 is pending
    assert wait_until(lambda: store.get(TP0) == 2)
    time.sleep(0.3)
    assert store == {TP0: 2}
    assert consumer_1.seek_calls == []

    finish_pipeline(thread_1, errors_1, end_event_1)
    assert store == {TP0: 2}
    assert consumer_1.closed

    # ---------------- run 2: resumes from the committed position ----------------
    end_event_2 = threading.Event()
    consumer_2 = FakeConsumer(data, store, batch_size=2, end_event=end_event_2)

    stage = pl.task.from_kafka(
        lambda: consumer_2, partitions=[("t", 0)], commit_interval=0.05
    )

    saved_2 = []

    def process_2(message):
        saved_2.append(message)
        return message.value

    stage = pl.task.map(process_2, stage, workers=1)

    thread_2, errors_2 = run_pipeline(stage)

    assert wait_until(lambda: len(saved_2) == 3)
    assert [message.offset for message in saved_2] == [2, 3, 4]
    assert consumer_2.seek_calls == [(TP0, 2)]

    # a late ack from run 1 must not affect the new run
    saved[0].ack()
    time.sleep(0.3)
    assert store == {TP0: 2}

    # acking run 2's own messages advances the committed offset
    for message in saved_2:
        message.ack()

    assert wait_until(lambda: store.get(TP0) == 5)

    finish_pipeline(thread_2, errors_2, end_event_2)
    assert store == {TP0: 5}
