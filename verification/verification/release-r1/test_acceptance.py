"""Observe release of a real partial output after a protocol failure, in both readers."""
import gc,tracemalloc
from websocket_checks import reader,frame,compressed

def test_error_releases_accumulated_message_output(reader):
    payload,_=compressed(b'R'*(4*1024*1024))
    parser,queue=reader(16*1024*1024)
    gc.collect();tracemalloc.start()
    try:
        assert parser.feed_data(frame(payload,compressed=True,fin=False))[0] is False
        gc.collect();before=tracemalloc.get_traced_memory()[0]
        assert before>3*1024*1024,'Probe did not observe substantial incremental output; investigate fixture before interpreting release'
        # A new binary data frame is illegal while a fragmented message is open.
        assert parser.feed_data(frame(b'x',opcode=2,fin=True))[0] is True
        assert queue.exception() is not None and not queue._buffer
        gc.collect();after,peak=tracemalloc.get_traced_memory()
        print({'partial_output_bytes':4*1024*1024,'current_before_error':before,'current_after_error':after,'peak':peak,'error_type':type(queue.exception()).__name__,'close_code':getattr(queue.exception(),'code',None)})
        assert after<1024*1024,'Reader retained the accumulated partial output after a protocol error'
        original=queue.exception()
        assert parser.feed_data(frame(b'later'))[0] is True
        assert queue.exception() is original and not queue._buffer
    finally:tracemalloc.stop()
