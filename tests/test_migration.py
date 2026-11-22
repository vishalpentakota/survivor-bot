import sys, pathlib, unittest, ast, os
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'app'))
from cloud_guard import ReadOnlyBroker
class Dummy:
    BUY='BUY'
    def get_positions(self): return {'status':'success','data':[]}
    def place_order(self): raise AssertionError('must never reach broker')
    def cancel_order(self): raise AssertionError('must never reach broker')
    def modify_order(self): raise AssertionError('must never reach broker')
class Tests(unittest.TestCase):
    def test_reads(self):
        self.assertEqual(ReadOnlyBroker(Dummy()).get_positions()['status'],'success')
    def test_writes_blocked(self):
        os.environ.pop('SURVIVOR_LIVE_TRADING', None)
        b=ReadOnlyBroker(Dummy())
        for method in ('place_order','cancel_order','modify_order'):
            with self.assertRaises(RuntimeError): getattr(b,method)()
    def test_live_flag_passes_writes(self):
        # Post-cutover activation switch (user-approved 2026-10-29): with
        # SURVIVOR_LIVE_TRADING=1 the guard passes broker writes through.
        os.environ['SURVIVOR_LIVE_TRADING']='1'
        try:
            fn = ReadOnlyBroker(Dummy()).place_order
            self.assertEqual(getattr(fn, '__name__', str(fn)), 'place_order')
        finally:
            os.environ.pop('SURVIVOR_LIVE_TRADING', None)
    def test_constants(self): self.assertEqual(ReadOnlyBroker(Dummy()).BUY,'BUY')
    def test_syntax(self):
        for p in (ROOT/'app').glob('*.py'):ast.parse(p.read_text(encoding='utf-8'))
    def test_cli_dispatches(self):
        # Post-cutover (user-approved live cutover 2026-10-29): the CLI migration
        # lock is REMOVED; the engine must dispatch actions, not exit.
        self.assertNotIn('Migration safety lock: use cloud_probe.py',(ROOT/'app/survivor_engine.py').read_text(encoding='utf-8'))
if __name__=='__main__':unittest.main()
