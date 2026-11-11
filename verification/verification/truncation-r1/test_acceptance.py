"""One exact stored-DEFLATE truncation with a known-valid local control."""
import pytest
from websocket_checks import reader, frame

VALID = b'\x00\x04\x00\xfb\xffDATA\x00'
TRUNCATED = VALID[:4]


def test_valid_manual_stored_block_has_exact_message_content(reader):
    parser, queue = reader(4096)
    assert parser.feed_data(frame(VALID, compressed=True))[0] is False
    assert [item.data for item in queue._buffer] == [b'DATA']


def test_stored_block_truncated_inside_length_header_is_rejected(reader):
    parser, queue = reader(4096)
    error, _ = parser.feed_data(frame(TRUNCATED, compressed=True))
    assert error is True, 'A truncated stored-block header became a complete message'
    original = queue.exception()
    assert original is not None and not queue._buffer
    print({'exception_type': type(original).__name__, 'close_code': getattr(original, 'code', None),
           'exact_truncated_bytes': TRUNCATED.hex()})
    assert parser.feed_data(frame(b'after-error'))[0] is True
    assert queue.exception() is original and not queue._buffer
