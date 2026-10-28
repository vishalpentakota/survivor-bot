import sys, pathlib, unittest, ast
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
        b=ReadOnlyBroker(Dummy())
        for method in ('place_order','cancel_order','modify_order'):
            with self.assertRaises(RuntimeError): getattr(b,method)()
    def test_constants(self): self.assertEqual(ReadOnlyBroker(Dummy()).BUY,'BUY')
    def test_syntax(self):
        for p in (ROOT/'app').glob('*.py'):ast.parse(p.read_text(encoding='utf-8'))
    def test_cli_lock(self):
        self.assertIn('Migration safety lock: use cloud_probe.py',(ROOT/'app/survivor_engine.py').read_text(encoding='utf-8'))
    def test_no_credentials(self):
        self.assertFalse((ROOT/'app/.env').exists())
        self.assertFalse((ROOT/'app/.dhan_token_state.json').exists())
if __name__=='__main__':unittest.main()
