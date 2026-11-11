"""Fixture-only zlib proof; does not import or grade a model artifact."""
import hashlib, json, zlib
from pathlib import Path

valid=b'\x00\x04\x00\xfb\xffDATA\x00'
truncated=valid[:4]
tail=b'\x00\x00\xff\xff'
assert zlib.decompressobj(wbits=-15).decompress(valid+tail)==b'DATA'
try:
    zlib.decompressobj(wbits=-15).decompress(truncated+tail)
except zlib.error as error:
    diagnostic=str(error)
else:raise AssertionError('The truncated fixture is not detectably invalid')
out=Path(__file__).parent/'preparation-selfcheck/websocket-stored-deflate-r1'
out.mkdir(exist_ok=False)
result={'kind':'FIXTURE_PREPARATION_NOT_MODEL_SCORE','valid_bytes':valid.hex(),'expected_output_hex':b'DATA'.hex(),
        'websocket_appended_tail':tail.hex(),'truncated_bytes':truncated.hex(),'actual_zlib_error':diagnostic}
(out/'fixture-proof.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
(out/'selfcheck_source.py').write_bytes(Path(__file__).read_bytes())
print(json.dumps(result))
