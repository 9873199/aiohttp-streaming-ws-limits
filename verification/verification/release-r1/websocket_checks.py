"""Locally generated frames: streaming limits and Python/compiled behavior parity."""
import asyncio,gc,struct,tracemalloc,zlib
from unittest.mock import Mock
import pytest
from aiohttp._websocket.reader_py import WebSocketReader as PythonReader,WebSocketDataQueue as PythonQueue
from aiohttp._websocket.reader_c import WebSocketReader as CompiledReader,WebSocketDataQueue as CompiledQueue

IMPLEMENTATIONS=[(PythonReader,PythonQueue),(CompiledReader,CompiledQueue)]
def frame(payload,opcode=2,fin=True,compressed=False):
    first=(128 if fin else 0)|(64 if compressed else 0)|opcode
    if len(payload)<126:return bytes((first,len(payload)))+payload
    if len(payload)<65536:return bytes((first,126))+struct.pack('!H',len(payload))+payload
    return bytes((first,127))+struct.pack('!Q',len(payload))+payload

def compressed(data,obj=None):
    obj=obj or zlib.compressobj(wbits=-15)
    result=obj.compress(data)+obj.flush(zlib.Z_SYNC_FLUSH)
    assert result.endswith(b'\x00\x00\xff\xff')
    return result[:-4],obj

@pytest.fixture(params=IMPLEMENTATIONS,ids=['python','compiled'])
def reader(request):
    cls,queue_cls=request.param;loop=asyncio.new_event_loop()
    def create(limit):
        queue=queue_cls(Mock(_reading_paused=False),2**20,loop=loop)
        return cls(queue,limit,compress=True,decode_text=True),queue
    yield create
    loop.close()

def test_compression_bomb_rejected_before_fin(reader):
    payload,_=compressed(b'A'*100000);parser,queue=reader(1024)
    error,_=parser.feed_data(frame(payload,fin=False,compressed=True))
    assert error is True,'Unfinished compressed message exceeded the limit without refusal'
    assert queue.exception().code==1009
    assert not queue._buffer,'Oversized message was delivered'
    assert parser.feed_data(frame(b'ignored'))[0] is True,'Reader continued normally after failure'

def test_control_frame_does_not_reset_compressed_message(reader):
    text=('fragmented-\u4f60\u597d-'*30).encode();payload,_=compressed(text);cut=max(1,len(payload)//2)
    parser,queue=reader(4096)
    for data in (frame(payload[:cut],opcode=1,fin=False,compressed=True),frame(b'ping',opcode=9),frame(b'',opcode=0,fin=False),frame(payload[cut:],opcode=0)):
        assert parser.feed_data(data)[0] is False
    messages=list(queue._buffer)
    assert any(m.type.value==9 and m.data==b'ping' for m in messages)
    assert [m.data for m in messages if m.type.value==1]==[text.decode()]

def test_context_takeover_two_messages_have_exact_content(reader):
    text=(b'public-dictionary-pattern-'*120)
    first,obj=compressed(text);second,_=compressed(text+b'next',obj)
    parser,queue=reader(10000)
    for payload in (first,second):assert parser.feed_data(frame(payload,compressed=True))[0] is False
    assert [m.data for m in queue._buffer]==[text,text+b'next']

@pytest.mark.parametrize('size',[1,3,7,10000])
def test_legal_utf8_message_every_input_size(reader,size):
    expected='\u5b89\u5168\u6d88\u606f\U0001f642'*30;payload,_=compressed(expected.encode());wire=frame(payload,opcode=1,compressed=True)
    parser,queue=reader(4096)
    for at in range(0,len(wire),size):assert parser.feed_data(wire[at:at+size])[0] is False
    assert [m.data for m in queue._buffer]==[expected]

def test_incremental_continuations_enforce_current_feed(reader):
    obj=zlib.compressobj(wbits=-15);parser,queue=reader(1024)
    a=obj.compress(b'A'*600)+obj.flush(zlib.Z_SYNC_FLUSH)
    b=obj.compress(b'B'*600)+obj.flush(zlib.Z_SYNC_FLUSH)
    assert parser.feed_data(frame(a,fin=False,compressed=True))[0] is False
    assert parser.feed_data(frame(b,opcode=0,fin=False))[0] is True
    assert queue.exception().code==1009 and not queue._buffer

def test_no_limit_preserves_existing_unbounded_setting(reader):
    payload,_=compressed(b'B'*100000);parser,queue=reader(0)
    assert parser.feed_data(frame(payload,compressed=True))[0] is False
    assert [m.data for m in queue._buffer]==[b'B'*100000]

def test_empty_continuations_do_not_retain_a_second_compressed_copy(reader):
    parser,queue=reader(1024);obj=zlib.compressobj(wbits=-15)
    first=obj.compress(b'')+obj.flush(zlib.Z_SYNC_FLUSH)
    assert parser.feed_data(frame(first,fin=False,compressed=True))[0] is False
    # Repeated empty compressor flushes may return b'', which would not test
    # retained compressed bytes. Inject valid nonfinal empty stored blocks.
    piece=b'\x00\x00\x00\xff\xff'
    probe=zlib.decompressobj(wbits=-15)
    assert probe.decompress(first+piece*3)==b''
    wire=frame(piece,opcode=0,fin=False)
    tracemalloc.start()
    try:
        for _ in range(1000):assert parser.feed_data(wire)[0] is False
        gc.collect();small=tracemalloc.get_traced_memory()[0]
        for _ in range(14000):assert parser.feed_data(wire)[0] is False
        gc.collect();large,peak=tracemalloc.get_traced_memory()
        print({'empty_frames':15000,'compressed_bytes':len(piece)*15000,'current_after_1000':small,'current_after_15000':large,'peak_bytes':peak})
        assert large-small<32768,'Parser retained growing compressed input despite empty decompressed output'
    finally:tracemalloc.stop()
    final=obj.compress(b'ok')+obj.flush(zlib.Z_SYNC_FLUSH)
    assert parser.feed_data(frame(final[:-4],opcode=0))[0] is False
    assert [m.data for m in queue._buffer]==[b'ok']

def test_large_expansion_has_bounded_allocation_on_the_rejecting_feed(reader):
    payload,_=compressed(b'Z'*(2**20));wire=frame(payload,fin=False,compressed=True)
    parser,queue=reader(4096);tracemalloc.start()
    try:
        error,_=parser.feed_data(wire);current,peak=tracemalloc.get_traced_memory()
        print({'decoded_fixture_bytes':2**20,'limit':4096,'current_bytes':current,'peak_bytes':peak})
        assert error is True and queue.exception().code==1009
        assert peak<512*1024,'Decoder allocated the full oversized output before rejecting it'
        assert not queue._buffer
        original_error=queue.exception()
        assert parser.feed_data(frame(b'valid-later-message'))[0] is True
        assert queue.exception() is original_error and not queue._buffer
    finally:tracemalloc.stop()
