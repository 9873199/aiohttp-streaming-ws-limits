"""Incremental permessage-deflate decompression in ``WebSocketReader``.

The compressed messages in this module are generated locally with zlib the
way a permessage-deflate peer with context takeover produces them
(RFC 7692): one deflate stream per message, the 0x00 0x00 0xff 0xff
sync-flush trailer stripped from the last fragment, and the sliding window
(dictionary) carried over between messages on the same connection.

Both reader implementations (pure Python and Cython) are fed the same
corpus and must produce the same messages, the same close codes and the
same residual state.
"""

import asyncio
import zlib
from collections.abc import Callable
from unittest import mock

import pytest

from aiohttp._websocket.helpers import PACK_LEN1, PACK_LEN2, PACK_LEN3
from aiohttp._websocket.models import WS_DEFLATE_TRAILING
from aiohttp._websocket.reader_py import (
    WebSocketDataQueue as PyWebSocketDataQueue,
    WebSocketReader as PyWebSocketReader,
)
from aiohttp.base_protocol import BaseProtocol
from aiohttp.http import HttpParser, WebSocketError, WSCloseCode, WSMsgType
from aiohttp.http_websocket import (
    WSMessageBinary,
    WSMessagePing,
    WSMessagePong,
    WSMessageText,
)
from aiohttp.streams import EofStream

try:
    from aiohttp._websocket.reader_c import (
        WebSocketDataQueue as CythonWebSocketDataQueue,
        WebSocketReader as CythonWebSocketReader,
    )
except ImportError:  # pragma: no cover
    CythonWebSocketDataQueue = None  # type: ignore[assignment, misc]
    CythonWebSocketReader = None  # type: ignore[assignment, misc]

IMPLEMENTATIONS = ["python"]
if CythonWebSocketReader is not None:
    IMPLEMENTATIONS.append("cython")


class DeflatePeer:
    """A permessage-deflate sender with context takeover.

    The compressor is kept across messages, so the deflate sliding window
    (dictionary) of earlier messages is used to compress later ones,
    exactly as negotiated without *_no_context_takeover.
    """

    def __init__(self) -> None:
        self._compressor = zlib.compressobj(wbits=-15)

    def compress_message(self, message: bytes) -> bytes:
        """Return the wire bytes of a whole message (trailer stripped)."""
        compressed = self._compressor.compress(message)
        compressed += self._compressor.flush(zlib.Z_SYNC_FLUSH)
        assert compressed.endswith(WS_DEFLATE_TRAILING)
        return compressed[:-4]


def frame(
    payload: bytes, opcode: WSMsgType, *, fin: bool = True, rsv1: bool = False
) -> bytes:
    """Build a single unmasked WebSocket frame."""
    first_byte = opcode | (0x80 if fin else 0) | (0x40 if rsv1 else 0)
    length = len(payload)
    if length < 126:
        return PACK_LEN1(first_byte, length) + payload
    if length < 65536:
        return PACK_LEN2(first_byte, 126, length) + payload
    return PACK_LEN3(first_byte, 127, length) + payload


def compressed_frames(
    peer: DeflatePeer, message: bytes, splits: tuple[int, ...] = ()
) -> list[bytes]:
    """Compress message and split the stream into frames at byte offsets.

    The first frame carries the message opcode and RSV1; the rest are
    continuation frames. Only the last frame has FIN set. Split points may
    coincide, producing empty continuation frames.
    """
    compressed = peer.compress_message(message)
    bounds = list(splits) + [len(compressed)]
    frames = []
    start = 0
    for index, end in enumerate(bounds):
        chunk = compressed[start:end]
        last = index == len(bounds) - 1
        if index == 0:
            frames.append(frame(chunk, WSMsgType.BINARY, fin=last, rsv1=True))
        else:
            frames.append(frame(chunk, WSMsgType.CONTINUATION, fin=last))
        start = end
    return frames


@pytest.fixture()
def protocol(event_loop: asyncio.AbstractEventLoop) -> BaseProtocol:
    parser = mock.create_autospec(HttpParser, spec_set=True, instance=True)
    transport = mock.Mock(spec_set=asyncio.Transport)
    protocol = BaseProtocol(event_loop, parser=parser)
    protocol.connection_made(transport)
    return protocol


MakeReader = Callable[[int], tuple[PyWebSocketReader, PyWebSocketDataQueue]]


@pytest.fixture(params=IMPLEMENTATIONS)
def make_reader(
    request: pytest.FixtureRequest, event_loop: asyncio.AbstractEventLoop
) -> MakeReader:
    """Build a reader/queue pair for the parametrized implementation."""
    if request.param == "cython":
        queue_cls = CythonWebSocketDataQueue
        reader_cls = CythonWebSocketReader
    else:
        queue_cls = PyWebSocketDataQueue
        reader_cls = PyWebSocketReader

    def _make(max_msg_size: int) -> tuple[PyWebSocketReader, PyWebSocketDataQueue]:
        queue = queue_cls(mock.Mock(_reading_paused=False), 2**16, loop=event_loop)
        return reader_cls(queue, max_msg_size, compress=True, decode_text=True), queue

    return _make


def test_multibyte_utf8_split_across_fragments(make_reader: MakeReader) -> None:
    """A multi-byte UTF-8 sequence straddling fragments decodes at message end."""
    reader, out = make_reader(4 * 1024 * 1024)
    # 𒀁 is U+12001, encoded as the four bytes f0 92 80 81. The first
    # fragment is flushed (a legal mid-stream sync flush) right after the
    # first of those bytes, so the character straddles the fragment boundary.
    message = "abc𒀁xyz".encode()
    prefix = message[:4]  # b"abc" + the lead byte of 𒀁
    compressor = zlib.compressobj(wbits=-15)
    first = compressor.compress(prefix) + compressor.flush(zlib.Z_SYNC_FLUSH)
    rest = compressor.compress(message[4:]) + compressor.flush(zlib.Z_SYNC_FLUSH)
    assert rest.endswith(WS_DEFLATE_TRAILING)
    # Sanity check the corpus: fragment one really does stop mid-character.
    check = zlib.decompressobj(wbits=-15)
    assert check.decompress(first) == prefix

    assert reader.feed_data(frame(first, WSMsgType.TEXT, fin=False, rsv1=True)) == (
        False,
        b"",
    )
    # Nothing is delivered (or decoded) before the final fragment.
    assert not out._buffer
    if isinstance(reader, PyWebSocketReader):
        # The decompressed prefix ends inside the 𒀁 sequence.
        assert bytes(reader._partial) == prefix
    assert reader.feed_data(
        frame(rest[:-4], WSMsgType.CONTINUATION, fin=True)
    ) == (False, b"")

    assert out._buffer[0] == WSMessageText(
        data="abc𒀁xyz", size=len(message), extra=""
    )
    if isinstance(reader, PyWebSocketReader):
        assert len(reader._partial) == 0  # released once the message is queued


def test_empty_continuation_frames(make_reader: MakeReader) -> None:
    """Empty continuation frames (including the FIN one) carry no stream bytes."""
    reader, out = make_reader(4 * 1024 * 1024)
    peer = DeflatePeer()
    message = b"payload delivered around empty continuations"
    compressed = peer.compress_message(message)
    third = len(compressed) // 3
    wire_frames = [
        frame(b"", WSMsgType.BINARY, fin=False, rsv1=True),  # empty first fragment
        frame(compressed[:third], WSMsgType.CONTINUATION, fin=False),
        frame(b"", WSMsgType.CONTINUATION, fin=False),  # empty, mid-message
        frame(compressed[third : 2 * third], WSMsgType.CONTINUATION, fin=False),
        frame(b"", WSMsgType.CONTINUATION, fin=False),
        frame(compressed[2 * third :], WSMsgType.CONTINUATION, fin=False),
        frame(b"", WSMsgType.CONTINUATION, fin=True),  # empty final fragment
    ]
    for wire in wire_frames[:-1]:
        assert reader.feed_data(wire) == (False, b"")
        assert not out._buffer  # nothing delivered before the FIN fragment

    # The FIN fragment is the last one fed, so the message is now complete.
    assert reader.feed_data(wire_frames[-1]) == (False, b"")
    assert out._buffer[0] == WSMessageBinary(
        data=message, size=len(message), extra=""
    )


def test_control_frames_do_not_disturb_compression(make_reader: MakeReader) -> None:
    """Interleaved control frames leave the message's compression state alone."""
    reader, out = make_reader(4 * 1024 * 1024)
    peer = DeflatePeer()
    message = b"interleaved control frames " * 8
    compressed = peer.compress_message(message)
    third = len(compressed) // 3

    assert reader.feed_data(
        frame(compressed[:third], WSMsgType.BINARY, fin=False, rsv1=True)
    ) == (False, b"")
    assert reader.feed_data(frame(b"ping-data", WSMsgType.PING)) == (False, b"")
    assert reader.feed_data(
        frame(compressed[third : 2 * third], WSMsgType.CONTINUATION, fin=False)
    ) == (False, b"")
    assert reader.feed_data(frame(b"", WSMsgType.PONG)) == (False, b"")
    assert reader.feed_data(
        frame(compressed[2 * third :], WSMsgType.CONTINUATION, fin=True)
    ) == (False, b"")

    # Control messages are delivered in order, ahead of the data message.
    assert out._buffer[0] == WSMessagePing(data=b"ping-data", size=9, extra="")
    assert out._buffer[1] == WSMessagePong(data=b"", size=0, extra="")
    assert out._buffer[2] == WSMessageBinary(
        data=message, size=len(message), extra=""
    )

    # The compression state survived the control frames: the very next
    # message still decompresses against the retained dictionary.
    followup = peer.compress_message(message)
    assert reader.feed_data(frame(followup, WSMsgType.BINARY, rsv1=True)) == (
        False,
        b"",
    )
    assert out._buffer[3] == WSMessageBinary(
        data=message, size=len(message), extra=""
    )


def test_context_takeover_dictionary_shared_between_messages(
    make_reader: MakeReader,
) -> None:
    """Consecutive messages decode against the retained deflate dictionary."""
    reader, out = make_reader(4 * 1024 * 1024)
    peer = DeflatePeer()
    # Incompressible content, so only the shared dictionary can make the
    # second message small.
    first_msg = bytes(range(256))
    second_msg = bytes(range(256)) + b"!"
    first_wire = peer.compress_message(first_msg)
    second_wire = peer.compress_message(second_msg)

    # The corpus must genuinely depend on the dictionary: the second message
    # is far smaller than its dictionary-less encoding, and decoding it with
    # a fresh decompressor fails because its back-references point into the
    # first message.
    naive = zlib.compressobj(wbits=-15)
    naive_wire = naive.compress(second_msg) + naive.flush(zlib.Z_SYNC_FLUSH)
    assert len(second_wire) < len(naive_wire) // 4
    with pytest.raises(zlib.error):
        zlib.decompressobj(wbits=-15).decompress(second_wire + WS_DEFLATE_TRAILING)

    assert reader.feed_data(frame(first_wire, WSMsgType.BINARY, rsv1=True)) == (
        False,
        b"",
    )
    assert reader.feed_data(frame(second_wire, WSMsgType.BINARY, rsv1=True)) == (
        False,
        b"",
    )

    assert out._buffer[0] == WSMessageBinary(
        data=first_msg, size=len(first_msg), extra=""
    )
    assert out._buffer[1] == WSMessageBinary(
        data=second_msg, size=len(second_msg), extra=""
    )
    if isinstance(reader, PyWebSocketReader):
        # The decompressor is retained across messages (context takeover).
        assert reader._decompressobj is not None


def _crossing_fragment(
    compressed: bytes, chunk: int, max_msg_size: int
) -> tuple[list[bytes], int]:
    """Split compressed into chunk-sized fragments; return them and the index
    of the fragment on which the cumulative decompressed size first exceeds
    max_msg_size, computed with a plain zlib walk of the same fragments."""
    fragments = [
        compressed[i : i + chunk] for i in range(0, len(compressed), chunk)
    ]
    decompressor = zlib.decompressobj(wbits=-15)
    total = 0
    for index, fragment in enumerate(fragments):
        total += len(decompressor.decompress(fragment))
        if total > max_msg_size:
            return fragments, index
    raise AssertionError("corpus never crosses the limit")


def test_over_limit_rejected_before_final_fragment(make_reader: MakeReader) -> None:
    """A peer streaming continuations without FIN is cut off mid-message.

    The 1009 close code must surface from the feed_data() call that crosses
    max_msg_size, not from the final fragment, and the decompressed buffer
    must never grow past the limit.
    """
    max_msg_size = 1024
    reader, out = make_reader(max_msg_size)
    peer = DeflatePeer()
    # Highly compressible: the whole stream is tiny, the message is not.
    message = b"A" * (64 * 1024)
    compressed = peer.compress_message(message)
    fragments, crossing = _crossing_fragment(compressed, 8, max_msg_size)
    # The corpus must cross the limit well before the final fragment, so the
    # FIN frame is never reached.
    assert crossing < len(fragments) - 2

    wire_frames = [
        frame(fragments[0], WSMsgType.BINARY, fin=False, rsv1=True),
        *(
            frame(fragment, WSMsgType.CONTINUATION, fin=False)
            for fragment in fragments[1:-1]
        ),
        frame(fragments[-1], WSMsgType.CONTINUATION, fin=True),
    ]

    high_water = 0
    for index, wire in enumerate(wire_frames[:crossing]):
        assert reader.feed_data(wire) == (False, b""), f"fragment {index}"
        assert not out._buffer
        if isinstance(reader, PyWebSocketReader):
            high_water = max(high_water, len(reader._partial))
            assert high_water <= max_msg_size

    # This exact feed_data() call crosses the limit and is rejected.
    assert reader.feed_data(wire_frames[crossing]) == (True, b"")
    exc = out.exception()
    assert isinstance(exc, WebSocketError)
    assert exc.code == WSCloseCode.MESSAGE_TOO_BIG  # 1009
    assert str(exc) == f"Decompressed message exceeds size limit {max_msg_size}"

    # Nothing was ever queued, so the queue's own counters stay at zero.
    assert not out._buffer
    assert out._size == 0

    # The failure released the in-flight message: no decompressed bytes, no
    # compressed copy, no deflate state is retained.
    if isinstance(reader, PyWebSocketReader):
        assert len(reader._partial) == 0
        assert not reader._payload_fragments
        assert not reader._payload_buffer
        assert reader._decompressobj is None

    # The reader is poisoned: further input (including the peer's FIN
    # fragment) is returned unconsumed instead of being parsed.
    fin_wire = wire_frames[-1]
    assert reader.feed_data(fin_wire) == (True, fin_wire)
    assert not out._buffer


def test_over_limit_single_frame_rejected(make_reader: MakeReader) -> None:
    """The same over-limit message in one FIN frame fails with the same code."""
    max_msg_size = 1024
    reader, out = make_reader(max_msg_size)
    peer = DeflatePeer()
    compressed = peer.compress_message(b"A" * (max_msg_size + 1))

    assert reader.feed_data(
        frame(compressed, WSMsgType.BINARY, rsv1=True)
    ) == (True, b"")
    exc = out.exception()
    assert isinstance(exc, WebSocketError)
    assert exc.code == WSCloseCode.MESSAGE_TOO_BIG
    assert not out._buffer


@pytest.mark.parametrize("over", (0, 1), ids=("at_limit", "one_over"))
def test_decompressed_size_limit_boundary(make_reader: MakeReader, over: int) -> None:
    """A message of exactly max_msg_size is accepted; one byte more is not."""
    max_msg_size = 512
    reader, out = make_reader(max_msg_size)
    peer = DeflatePeer()
    message = b"B" * (max_msg_size + over)
    wire_frames = compressed_frames(peer, message, splits=(3, 7))

    results = [reader.feed_data(wire) for wire in wire_frames]
    if over:
        assert results[-1] == (True, b"")
        exc = out.exception()
        assert isinstance(exc, WebSocketError)
        assert exc.code == WSCloseCode.MESSAGE_TOO_BIG
        assert not out._buffer
    else:
        assert results == [(False, b"")] * len(wire_frames)
        assert out._buffer[0] == WSMessageBinary(
            data=message, size=len(message), extra=""
        )
        if isinstance(reader, PyWebSocketReader):
            assert len(reader._partial) == 0


def test_corrupt_deflate_stream_rejected(make_reader: MakeReader) -> None:
    """Invalid deflate bytes fail the reader and release the in-flight state."""
    reader, out = make_reader(4 * 1024 * 1024)
    peer = DeflatePeer()
    compressed = peer.compress_message(b"a valid prefix")

    # First fragment is fine; the second is garbage (invalid block type).
    assert reader.feed_data(
        frame(compressed, WSMsgType.BINARY, fin=False, rsv1=True)
    ) == (False, b"")
    assert reader.feed_data(
        frame(b"\xff" * 8, WSMsgType.CONTINUATION, fin=True)
    ) == (True, b"")

    exc = out.exception()
    assert isinstance(exc, zlib.error)
    assert not out._buffer

    # The failure released the in-flight message and the deflate state.
    if isinstance(reader, PyWebSocketReader):
        assert len(reader._partial) == 0
        assert not reader._payload_fragments
        assert not reader._payload_buffer
        assert reader._decompressobj is None

    # The reader is poisoned: further input is returned unconsumed.
    more = frame(b"hello", WSMsgType.TEXT)
    assert reader.feed_data(more) == (True, more)
    assert not out._buffer


def test_truncated_deflate_stream_yields_prefix(make_reader: MakeReader) -> None:
    """A deflate stream cut short and then FINned decodes to what arrived.

    Raw deflate has no end-of-stream integrity check at a sync-flush
    boundary, so a truncated message is delivered with the content that
    could be inflated; integrity above that is the application's concern.
    """
    reader, out = make_reader(4 * 1024 * 1024)
    peer = DeflatePeer()
    message = b"truncated deflate stream test" * 3
    compressed = peer.compress_message(message)
    half = compressed[: len(compressed) // 2]
    expected = zlib.decompressobj(wbits=-15).decompress(half + WS_DEFLATE_TRAILING)
    assert 0 < len(expected) < len(message)

    assert reader.feed_data(frame(half, WSMsgType.BINARY, rsv1=True)) == (False, b"")
    assert out._buffer[0] == WSMessageBinary(
        data=expected, size=len(expected), extra=""
    )


async def test_eof_mid_message_drops_partial_message(make_reader: MakeReader) -> None:
    """A compressed message that never gets its FIN is never delivered."""
    reader, out = make_reader(4 * 1024 * 1024)
    peer = DeflatePeer()
    compressed = peer.compress_message(b"never finished")

    assert reader.feed_data(
        frame(compressed, WSMsgType.TEXT, fin=False, rsv1=True)
    ) == (False, b"")
    reader.feed_eof()

    assert out.is_eof()
    assert not out._buffer
    with pytest.raises(EofStream):
        await out.read()
