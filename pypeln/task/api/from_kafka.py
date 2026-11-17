import asyncio
import collections
import inspect
import itertools
import logging
import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import T

from ..stage import Stage
from ..worker import Worker

logger = logging.getLogger(__name__)


class TopicPartition(tp.NamedTuple):
    """
    Identifies a single Kafka partition. Plain `(topic, partition)` tuples are
    accepted anywhere a `TopicPartition` is expected.
    """

    topic: str
    partition: int


class KafkaMessage(tp.Generic[T]):
    """
    A single Kafka record delivered downstream by `from_kafka`.

    The message wraps the raw record returned by the consumer and carries an
    explicit `ack()` handle. Downstream workers (e.g. `pl.task.map` /
    `pl.task.each`) must call `ack()` once the record has been fully
    processed; only then can its offset become part of a committable
    contiguous prefix. Acks may be performed out of order and from any
    thread, but a partition's committed offset never skips a
    delivered-but-unacknowledged record, so records that complete later
    never cause earlier pending records to be skipped.
    """

    def __init__(
        self,
        record: tp.Any,
        ack_fn: tp.Callable[[TopicPartition, int, int], None],
        generation: int,
        loop: asyncio.AbstractEventLoop,
    ):
        self.record = record
        self.value = getattr(record, "value", record)
        self.topic = record.topic
        self.partition = record.partition
        self.offset = record.offset
        self.key = getattr(record, "key", None)
        self.headers = getattr(record, "headers", None)
        self.timestamp = getattr(record, "timestamp", None)

        self._ack_fn = ack_fn
        self._generation = generation
        self._loop = loop
        self._acked = False

    @property
    def acked(self) -> bool:
        """
        `bool`: `True` if `ack()` was already called on this message.
        """
        return self._acked

    def ack(self) -> bool:
        """
        Explicitly acknowledges this record. It is idempotent and safe to
        call from any thread. Acks are delivered to the collector worker's
        event loop, so they keep being processed even when downstream
        backpressure has paused polling.

        Acks of messages produced by a previous run are ignored and never
        affect a newer run.

        Returns:
            `True` if the ack was accepted for delivery, `False` if it was
            a duplicate or the run that produced the message already ended.
        """
        if self._acked:
            return False

        self._acked = True
        tp = TopicPartition(self.topic, self.partition)

        try:
            self._loop.call_soon_threadsafe(
                self._ack_fn, tp, self.offset, self._generation
            )
        except RuntimeError:
            # The event loop of the run that produced this message is gone:
            # this is a late ack from an old run and must be ignored.
            return False

        return True

    def __repr__(self) -> str:
        return (
            f"KafkaMessage(topic={self.topic!r}, partition={self.partition}, "
            f"offset={self.offset}, acked={self._acked})"
        )


class _PartitionState:
    """
    Tracks the delivered/acknowledged offsets of a single partition and
    computes its committable point: the first delivered-but-unacknowledged
    offset, or one past the highest delivered offset when everything
    delivered was acknowledged. Only delivered offsets are tracked, so gaps
    in the broker's offset sequence are never treated as pending records.
    """

    __slots__ = ("committed", "first_delivered", "max_delivered", "unacked", "order")

    def __init__(self, committed: int = -1):
        self.committed = committed
        self.first_delivered: tp.Optional[int] = None
        self.max_delivered = -1
        self.unacked: tp.Set[int] = set()
        self.order: tp.Deque[int] = collections.deque()

    def deliver(self, offset: int):
        if self.first_delivered is None:
            self.first_delivered = offset

        self.max_delivered = max(self.max_delivered, offset)
        self.unacked.add(offset)
        self.order.append(offset)

    def ack(self, offset: int) -> bool:
        if offset not in self.unacked:
            return False

        self.unacked.remove(offset)

        while self.order and self.order[0] not in self.unacked:
            self.order.popleft()

        return True

    @property
    def commit_point(self) -> int:
        if self.order:
            return self.order[0]

        return self.max_delivered + 1


class OffsetTracker:
    """
    Computes safe per-partition commit offsets from out-of-order acks.

    Each partition advances independently and only the contiguous
    acknowledged prefix of *delivered* records is ever committed: a later
    record completing first never lets the committed offset skip an earlier
    pending record.

    Every pipeline run owns a fresh tracker with a unique `generation`.
    Acks stamped with a different generation (late acks from an old run),
    or arriving after the tracker was closed, are ignored.
    """

    _generations = itertools.count()

    def __init__(self):
        self.generation = next(OffsetTracker._generations)
        self.closed = False
        self._states: tp.Dict[TopicPartition, _PartitionState] = {}

    def set_committed(self, tp: TopicPartition, committed: int):
        self._states.setdefault(tp, _PartitionState(committed=committed))

    def deliver(self, tp: TopicPartition, offset: int):
        state = self._states.get(tp)

        if state is None:
            state = self._states[tp] = _PartitionState()

        state.deliver(offset)

    def ack(self, generation: int, tp: TopicPartition, offset: int) -> bool:
        if self.closed or generation != self.generation:
            return False

        state = self._states.get(tp)

        if state is None:
            return False

        return state.ack(offset)

    def pending_commits(self) -> tp.Dict[TopicPartition, int]:
        commits = {}

        for tp, state in self._states.items():
            if state.first_delivered is None:
                continue

            point = state.commit_point

            # A commit only represents progress: the acknowledged prefix must
            # move past both the last committed offset and the first delivered
            # offset (before any ack the frontier just restates the position
            # the run started from, which is not progress).
            if point > state.committed and point > state.first_delivered:
                commits[tp] = point

        return commits

    def mark_committed(self, offsets: tp.Dict[TopicPartition, int]):
        for tp, offset in offsets.items():
            state = self._states.get(tp)

            if state is not None and offset > state.committed:
                state.committed = offset

    def close(self):
        self.closed = True


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value

    return value


async def _call_optional(obj: tp.Any, name: str, *args):
    method = getattr(obj, name, None)

    if method is None:
        return None

    return await _maybe_await(method(*args))


class FromKafka(tp.NamedTuple):
    consumer_factory: tp.Callable[[], tp.Any]
    topics: tp.Tuple[str, ...]
    partitions: tp.Tuple[TopicPartition, ...]
    max_in_flight: int
    commit_interval: float
    poll_timeout: float

    async def __call__(self, worker: Worker, **kwargs):
        # The collector worker is the *only* place where the consumer is
        # touched: it exclusively owns poll, commit and close. Downstream
        # workers only acknowledge messages through `handle_ack`, which is
        # always scheduled back into this worker's event loop.
        consumer = self.consumer_factory()
        loop = asyncio.get_running_loop()

        tracker = OffsetTracker()
        ack_event = asyncio.Event()
        permits = asyncio.Semaphore(self.max_in_flight)

        def handle_ack(tp: TopicPartition, offset: int, generation: int):
            if tracker.ack(generation, tp, offset):
                permits.release()
                ack_event.set()

        await _call_optional(consumer, "start")

        partitions = await self._resolve_partitions(consumer)

        # Fixed assignment: partitions are assigned once and never rebalanced.
        await _maybe_await(consumer.assign(list(partitions)))

        # Resume from the last committed position of each partition.
        for tp in partitions:
            committed = await _maybe_await(consumer.committed(tp))

            if committed is None:
                committed = -1
            else:
                await _maybe_await(consumer.seek(tp, committed))

            tracker.set_committed(tp, committed)

        async def poll_loop():
            index = 0

            while True:
                records = await _maybe_await(consumer.poll(self.poll_timeout))

                for record in records:
                    # Bounds the amount of delivered-but-uncommitted records.
                    await permits.acquire()

                    tp = TopicPartition(record.topic, record.partition)
                    tracker.deliver(tp, record.offset)

                    message = KafkaMessage(
                        record=record,
                        ack_fn=handle_ack,
                        generation=tracker.generation,
                        loop=loop,
                    )

                    await worker.stage_params.output_queues.put(
                        pypeln_utils.Element(index=(index,), value=message)
                    )

                    index += 1

        async def flush_commits():
            offsets = tracker.pending_commits()

            if not offsets:
                return

            await _maybe_await(consumer.commit(offsets))
            tracker.mark_committed(offsets)

        async def commit_loop():
            # Driven by ack events and by a periodic timer, so commits happen
            # independently of whether new messages are arriving and of
            # downstream backpressure.
            while True:
                try:
                    await asyncio.wait_for(ack_event.wait(), self.commit_interval)
                except asyncio.TimeoutError:
                    pass

                ack_event.clear()
                await flush_commits()

        poller = asyncio.ensure_future(poll_loop())
        committer = asyncio.ensure_future(commit_loop())

        try:
            done, _pending = await asyncio.wait(
                [poller, committer], return_when=asyncio.FIRST_EXCEPTION
            )

            for task in done:
                if task.cancelled():
                    continue

                exception = task.exception()

                if exception is not None:
                    raise exception
        finally:
            poller.cancel()
            committer.cancel()
            await asyncio.gather(poller, committer, return_exceptions=True)

            tracker.close()

            try:
                await flush_commits()
            except Exception:
                logger.warning(
                    "pypeln: failed to commit Kafka offsets during shutdown",
                    exc_info=True,
                )

            await _call_optional(consumer, "close")

    async def _resolve_partitions(self, consumer) -> tp.List[TopicPartition]:
        partitions = list(self.partitions)

        if not partitions:
            seen = set()

            for topic in self.topics:
                found = await _maybe_await(consumer.partitions_for_topic(topic))

                for partition in found or []:
                    tp = TopicPartition(topic, partition)

                    if tp not in seen:
                        seen.add(tp)
                        partitions.append(tp)

            partitions.sort()

        if not partitions:
            raise ValueError(
                f"Could not find any partitions for topics {list(self.topics)}"
            )

        return partitions


def from_kafka(
    consumer_factory: tp.Callable[[], tp.Any],
    topics: tp.Iterable[str] = (),
    partitions: tp.Iterable[tp.Union[TopicPartition, tp.Tuple[str, int]]] = (),
    max_in_flight: int = 1024,
    commit_interval: float = 1.0,
    poll_timeout: float = 0.5,
    maxsize: int = 0,
) -> Stage:
    """
    Creates a source stage that consumes Kafka records with fixed partition
    assignment and explicit acknowledgment, designed for parallel pipelines
    where records complete out of order.

    A single collector worker exclusively owns the consumer (`poll`, `commit`
    and `close`). Records are delivered downstream as `KafkaMessage` objects
    which downstream workers acknowledge explicitly via `message.ack()`, in
    any order. Per partition only the contiguous acknowledged prefix of
    delivered records is committed, so an offset is never skipped because a
    later record finished first; gaps in the offset sequence are not treated
    as pending records and each partition advances independently. Acks and
    commits keep being processed while downstream backpressure pauses
    polling, the number of in-flight uncommitted records is bounded by
    `max_in_flight`, and commit failures are raised as pipeline exceptions.
    Auto-commit is not used, so a restarted run resumes from the last
    committed position and late acks from a previous run are ignored.

    ```python
    import pypeln as pl

    stage = pl.task.from_kafka(
        consumer_factory=lambda: pl.task.AiokafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="cleaners",
        ),
        topics=["raw-records"],
        max_in_flight=512,
    )

    def clean(message):
        record = transform(message.value)
        message.ack()  # explicit acknowledgment, safe to call out of order
        return record

    stage = pl.task.map(clean, stage, workers=8)
    pl.task.run(stage)
    ```

    !!! note
        `consumer_factory` is called once per pipeline run and must return a
        *new* consumer implementing the following protocol (`start` and
        `close` are optional, every method may be a coroutine):

        * `start()`: called once before anything else.
        * `assign(partitions)`: fixed assignment of `List[TopicPartition]`.
        * `committed(tp) -> Optional[int]`: last committed offset for `tp`.
        * `seek(tp, offset)`: set the fetch position.
        * `poll(timeout) -> List[record]`: wait up to `timeout` seconds for
          records; records must expose `topic`, `partition`, `offset` and
          `value` attributes.
        * `commit(offsets)`: commit `Dict[TopicPartition, int]`; failures
          must be raised so they stay visible.
        * `close()`: called once at the end of the run.
        * `partitions_for_topic(topic) -> Iterable[int]`: only needed when
          `partitions` is not given.

        `pl.task.AiokafkaConsumer` implements this protocol on top of
        `aiokafka` with auto-commit disabled.

    Arguments:
        consumer_factory: A zero-argument callable returning a new consumer
            (see the protocol above) for each run.
        topics: Topics to consume. All their partitions are discovered once
            and assigned statically. Ignored if `partitions` is given.
        partitions: Explicit `(topic, partition)` pairs to consume. If
            empty, partitions are discovered from `topics`.
        max_in_flight: Maximum number of records delivered but not yet
            acknowledged; polling pauses while the bound is reached.
        commit_interval: Maximum seconds between commit attempts. Commits
            are also triggered by acks, so they never depend on the arrival
            of new messages.
        poll_timeout: Seconds each `poll` call waits for new records.
        maxsize: The maximum number of objects the stage can hold
            simultaneously, if set to `0` (default) it can grow unbounded.

    Returns:
        Returns a `Stage` yielding `KafkaMessage` objects.
    """

    if not callable(consumer_factory):
        raise TypeError("consumer_factory must be a callable returning a consumer")

    topics = tuple(topics)
    partitions = tuple(
        TopicPartition(topic, partition) for topic, partition in partitions
    )

    if not topics and not partitions:
        raise ValueError("Either topics or partitions must be given")

    if max_in_flight < 1:
        raise ValueError("max_in_flight must be at least 1")

    if commit_interval <= 0:
        raise ValueError("commit_interval must be greater than 0")

    if poll_timeout < 0:
        raise ValueError("poll_timeout must be greater than or equal to 0")

    return Stage(
        process_fn=FromKafka(
            consumer_factory=consumer_factory,
            topics=topics,
            partitions=partitions,
            max_in_flight=max_in_flight,
            commit_interval=commit_interval,
            poll_timeout=poll_timeout,
        ),
        workers=1,
        maxsize=maxsize,
        total_sources=1,
        timeout=0,
        dependencies=[],
        on_start=None,
        on_done=None,
        f_args=[],
    )


class AiokafkaConsumer:
    """
    `from_kafka`-compatible consumer on top of
    [aiokafka](https://github.com/aio-libs/aiokafka).

    Auto-commit is always disabled and partitions are assigned manually
    (fixed assignment, no consumer-group rebalancing). Pass the
    `AIOKafkaConsumer` configuration as keyword arguments, but do **not**
    pass topics to the constructor: pass them to `from_kafka` instead.

    ```python
    stage = pl.task.from_kafka(
        consumer_factory=lambda: pl.task.AiokafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="cleaners",
            auto_offset_reset="earliest",
        ),
        topics=["raw-records"],
    )
    ```
    """

    def __init__(self, *args, **config):
        if args:
            raise ValueError(
                "AiokafkaConsumer does not accept topics; "
                "pass them to from_kafka(..., topics=...) instead"
            )

        config["enable_auto_commit"] = False

        self._config = config
        self._consumer = None

    async def start(self):
        from aiokafka import AIOKafkaConsumer

        self._consumer = AIOKafkaConsumer(**self._config)
        await self._consumer.start()

    def assign(self, partitions: tp.List[TopicPartition]):
        from aiokafka import TopicPartition as AioTopicPartition

        self._consumer.assign(
            [AioTopicPartition(tp.topic, tp.partition) for tp in partitions]
        )

    def partitions_for_topic(self, topic: str) -> tp.Iterable[int]:
        return self._consumer.partitions_for_topic(topic)

    async def committed(self, tp: TopicPartition) -> tp.Optional[int]:
        from aiokafka import TopicPartition as AioTopicPartition

        return await _maybe_await(
            self._consumer.committed(AioTopicPartition(tp.topic, tp.partition))
        )

    def seek(self, tp: TopicPartition, offset: int):
        from aiokafka import TopicPartition as AioTopicPartition

        self._consumer.seek(AioTopicPartition(tp.topic, tp.partition), offset)

    async def poll(self, timeout: float) -> tp.List[tp.Any]:
        records = await self._consumer.getmany(timeout_ms=int(timeout * 1000))

        return [record for batch in records.values() for record in batch]

    async def commit(self, offsets: tp.Dict[TopicPartition, int]):
        from aiokafka import TopicPartition as AioTopicPartition

        try:
            from aiokafka import OffsetAndMetadata
        except ImportError:
            from aiokafka.structs import OffsetAndMetadata

        await self._consumer.commit(
            {
                AioTopicPartition(tp.topic, tp.partition): OffsetAndMetadata(offset, "")
                for tp, offset in offsets.items()
            }
        )

    async def close(self):
        await self._consumer.stop()
