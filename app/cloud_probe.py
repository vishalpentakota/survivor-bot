"""Read-only network/broker/feed probe. Never calls tick/status/premarket."""
import argparse, json, datetime, os
from pathlib import Path
import requests
p=argparse.ArgumentParser()
p.add_argument('--broker', action='store_true')
a=p.parse_args()
print('UTC:',datetime.datetime.now(datetime.timezone.utc).isoformat())
print('Workspace:',Path(__file__).resolve().parent)
r=requests.get('https://api.ipify.org',timeout=15); r.raise_for_status()
print('Current outbound IPv4 (not guaranteed static):',r.text.strip())
if a.broker:
    import survivor_engine as s
    c=s.SurvivorDhanClient()
    h={'access-token':c.tm.get_token(),'client-id':c.tm.client_id}
    r=requests.get('https://api.dhan.co/v2/ip/getIP',headers=h,timeout=15);r.raise_for_status()
    print('Dhan whitelist:',json.dumps(r.json()))
    for name,fn in [('funds',c.dhan.get_fund_limits),('positions',c.dhan.get_positions),('orders',c.dhan.get_order_list)]:
        result=fn()
        if result.get('status')!='success':raise RuntimeError(name+' API failed')
        print(name,json.dumps(result))
    for name,cfg in s.INDEX_UNIVERSE.items():
        d=c.fetch_candles(cfg['security_id'],exchange_segment='IDX_I',instrument_type='INDEX',days_back=5,resample_tf='3min')
        print(name,'bars',len(d),'last_timestamp',str(d.index[-1]) if len(d) else 'NONE')
else:
    print('Broker skipped. Configure private credentials, then use --broker for read-only checks.')
