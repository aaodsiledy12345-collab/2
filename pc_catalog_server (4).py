#!/usr/bin/env python3
"""PC quote API: licensed CSV catalogue + optional live Danawa data via ReefAPI.
No synthetic/default prices are generated. Provider credentials remain server-side.
"""
from __future__ import annotations
import argparse, csv, json, os, re, sqlite3, sys, threading, time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen

BASE = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get('PC_CATALOG_DB', str(BASE / 'pc_catalog.sqlite3')))
REEF_API_KEY = os.environ.get('REEF_API_KEY', '').strip()
REEF_API_BASE = os.environ.get('REEF_API_BASE', 'https://api.reefapi.com').rstrip('/')
PRICE_CACHE_SECONDS = max(30, int(os.environ.get('PRICE_CACHE_SECONDS', '300')))
DB_LOCK = threading.RLock()
PRICE_CACHE = {}
# Cache normalized search results so UI pages of 24 products can be assembled
# from ReefAPI's 40-row upstream pages without skipping products between pages.
LIVE_SEARCH_CACHE = {}
LIVE_SEARCH_LOCK = threading.RLock()
LIVE_SEARCH_CACHE_TTL = max(60, PRICE_CACHE_SECONDS)

CATEGORY_MAP = {
 'cpu':'cpu','processor':'cpu','프로세서':'cpu','중앙처리장치':'cpu',
 'motherboard':'motherboard','mainboard':'motherboard','메인보드':'motherboard',
 'ram':'ram','memory':'ram','메모리':'ram','gpu':'gpu','vga':'gpu','graphics':'gpu','그래픽카드':'gpu',
 'ssd':'ssd','hdd':'hdd','cooler':'cooler','cooling':'cooler','쿨러':'cooler','튜닝':'cooler',
 'case':'case','케이스':'case','psu':'psu','power':'psu','파워':'psu','software':'software','소프트웨어':'software','cable':'cable','케이블':'cable'
}

def now_iso(): return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
def valid_url(v):
 v=str(v or '').strip(); return v if v.startswith(('https://','http://')) else ''
def price_num(v):
 try: return int(float(str(v).replace(',','').replace('₩','').strip()))
 except (ValueError,TypeError): return 0

def connect():
 c=sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); c.row_factory=sqlite3.Row
 c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); return c

def init_db():
 with DB_LOCK, connect() as db:
  db.execute('''CREATE TABLE IF NOT EXISTS products(id TEXT PRIMARY KEY, category TEXT NOT NULL, brand TEXT NOT NULL DEFAULT '', name TEXT NOT NULL, price_krw INTEGER NOT NULL DEFAULT 0, image_url TEXT NOT NULL DEFAULT '', product_url TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '', socket TEXT NOT NULL DEFAULT '', memory_type TEXT NOT NULL DEFAULT '', form_factor TEXT NOT NULL DEFAULT '', capacity REAL NOT NULL DEFAULT 0, wattage REAL NOT NULL DEFAULT 0, tdp REAL NOT NULL DEFAULT 0, length_mm REAL NOT NULL DEFAULT 0, height_mm REAL NOT NULL DEFAULT 0, max_gpu_mm REAL NOT NULL DEFAULT 0, max_cooler_mm REAL NOT NULL DEFAULT 0, fan_size_mm REAL NOT NULL DEFAULT 0, cores REAL NOT NULL DEFAULT 0, threads REAL NOT NULL DEFAULT 0, slots_m2 REAL NOT NULL DEFAULT 0, specs_json TEXT NOT NULL DEFAULT '{}', updated_at TEXT NOT NULL)''')
  db.execute('CREATE INDEX IF NOT EXISTS idx_products_category_brand ON products(category,brand)')
  try:
   db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS product_search USING fts5(name,brand,description,content='products',content_rowid='rowid')")
   db.executescript('''CREATE TRIGGER IF NOT EXISTS products_ai AFTER INSERT ON products BEGIN INSERT INTO product_search(rowid,name,brand,description) VALUES(new.rowid,new.name,new.brand,new.description); END;
   CREATE TRIGGER IF NOT EXISTS products_ad AFTER DELETE ON products BEGIN INSERT INTO product_search(product_search,rowid,name,brand,description) VALUES('delete',old.rowid,old.name,old.brand,old.description); END;
   CREATE TRIGGER IF NOT EXISTS products_au AFTER UPDATE OF name,brand,description ON products BEGIN INSERT INTO product_search(product_search,rowid,name,brand,description) VALUES('delete',old.rowid,old.name,old.brand,old.description); INSERT INTO product_search(rowid,name,brand,description) VALUES(new.rowid,new.name,new.brand,new.description); END;''')
  except sqlite3.OperationalError: pass

def import_catalog_csv(path: Path):
 if not path.exists(): raise FileNotFoundError(f'CSV 파일을 찾을 수 없습니다: {path}')
 fields=['id','category','brand','name','price_krw','image_url','product_url','description','socket','memory_type','form_factor','capacity','wattage','tdp','length_mm','height_mm','max_gpu_mm','max_cooler_mm','fan_size_mm','cores','threads','slots_m2','specs_json','updated_at']
 sql=f"INSERT INTO products ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)}) ON CONFLICT(id) DO UPDATE SET "+','.join(f'{x}=excluded.{x}' for x in fields if x!='id')
 n=0
 with path.open('r',encoding='utf-8-sig',newline='') as f, DB_LOCK, connect() as db:
  reader=csv.DictReader(f); required={'id','category','brand','name','priceKrw','imageUrl','url'}; absent=required-set(reader.fieldnames or [])
  if absent: raise ValueError('CSV 필수 열이 없습니다: '+', '.join(sorted(absent)))
  for r in reader:
   pid=(r.get('id') or r.get('sku') or '').strip(); name=(r.get('name') or '').strip(); cat=CATEGORY_MAP.get((r.get('category') or '').strip().lower(),'')
   if not pid or not name or not cat: continue
   try: specs=json.dumps(json.loads(r.get('specsJson') or '{}'),ensure_ascii=False)
   except Exception: specs='{}'
   vals=[pid,cat,(r.get('brand') or '').strip(),name,price_num(r.get('priceKrw')),valid_url(r.get('imageUrl')),valid_url(r.get('url')),(r.get('description') or '').strip(),(r.get('socket') or '').strip(),(r.get('memory') or '').strip(),(r.get('formFactor') or '').strip(),price_num(r.get('capacity')),price_num(r.get('wattage')),price_num(r.get('tdp')),price_num(r.get('lengthMm')),price_num(r.get('heightMm')),price_num(r.get('maxGpuLengthMm')),price_num(r.get('maxCoolerHeightMm')),price_num(r.get('fanSizeMm')),price_num(r.get('cores')),price_num(r.get('threads')),price_num(r.get('slotsM2')),specs,now_iso()]
   db.execute(sql,vals); n+=1
  db.commit()
 print(f'CSV 가져오기 완료: {n:,}행 처리'); return n

def count_products():
 with DB_LOCK, connect() as db: return db.execute('SELECT COUNT(*) FROM products').fetchone()[0]

def reef_post(endpoint, payload):
 if not REEF_API_KEY: raise RuntimeError('REEF_API_KEY가 설정되지 않았습니다. 서버 환경변수에 API 키를 설정하세요.')
 req=Request(REEF_API_BASE+endpoint, data=json.dumps(payload).encode(), headers={'x-api-key':REEF_API_KEY,'Content-Type':'application/json','Accept':'application/json','User-Agent':'PCQuoteLiveCatalog/1.0'}, method='POST')
 with urlopen(req,timeout=25) as r: body=json.loads(r.read().decode('utf-8'))
 if isinstance(body,dict) and body.get('ok') is False: raise RuntimeError(str(body.get('error') or '가격 공급사 API 오류'))
 return body.get('data',body) if isinstance(body,dict) else body

def recursive_dicts(obj):
 if isinstance(obj,dict):
  yield obj
  for v in obj.values(): yield from recursive_dicts(v)
 elif isinstance(obj,list):
  for v in obj: yield from recursive_dicts(v)

def extract_products(data):
 # Normalize actual supplier records only; a real image URL is required.
 candidates=[]
 for x in recursive_dicts(data):
  name=str(x.get('name') or x.get('product_name') or x.get('title') or x.get('productName') or '').strip()
  pid=str(x.get('product_id') or x.get('productId') or x.get('pcode') or x.get('id') or '').strip()
  if not name or not pid: continue
  img=valid_url(x.get('image_url') or x.get('imageUrl') or x.get('image') or x.get('thumbnail') or x.get('img'))
  url=valid_url(x.get('url') or x.get('product_url') or x.get('productUrl'))
  if not url and pid.isdigit(): url='https://prod.danawa.com/info/?pcode='+pid
  if not img: continue
  price=price_num(x.get('lowest_price') or x.get('price') or x.get('lowestPrice') or x.get('price_krw'))
  brand=str(x.get('maker') or x.get('brand') or x.get('manufacturer') or '').strip()
  category='ssd' if re.search(r'\bssd\b|NVMe',name,re.I) else 'hdd' if re.search(r'\bHDD\b|하드디스크',name,re.I) else 'ram' if re.search(r'\bDDR[3454]\b|메모리|램',name,re.I) else 'cpu' if re.search(r'라이젠|코어\s*울트라|코어\s*i[3579]|Core\s*i[3579]|Xeon|프로세서|Threadripper',name,re.I) else 'gpu' if re.search(r'RTX|GTX|Radeon|라데온|그래픽카드|GeForce',name,re.I) else 'motherboard' if re.search(r'메인보드|마더보드|motherboard|B[456789]\d{2}|X[3678]\d{2}|Z[6789]\d{2}',name,re.I) else 'psu' if re.search(r'파워서플라이|전원공급장치|\b\d{3,4}W\b',name,re.I) else 'case' if re.search(r'PC 케이스|컴퓨터 케이스|미들타워|빅타워',name,re.I) else 'cooler' if re.search(r'쿨러|수랭|수냉|라디에이터',name,re.I) else 'other'
  candidates.append({'id':'danawa:'+pid,'externalId':pid,'category':category,'brand':brand,'name':name,'priceKrw':price or None,'currency':'KRW','imageUrl':img,'url':url,'source':'다나와 가격비교 API','updatedAt':now_iso(),'priceStatus':'live' if price else 'not_available'})
 # Deduplicate supplier IDs, preserving order.
 seen=set(); out=[]
 for x in candidates:
  if x['id'] not in seen: seen.add(x['id']); out.append(x)
 return out

def extract_total(data, fallback):
 # Preserve the supplier's catalogue-wide count when one is provided.
 keys=('total','total_count','totalCount','totalResults','total_results','totalProducts','total_products','count')
 for obj in recursive_dicts(data):
  for key in keys:
   value=obj.get(key)
   if isinstance(value, (int,float)) and value >= 0: return int(value)
   if isinstance(value,str) and value.strip().isdigit(): return int(value.strip())
 return int(fallback)

def cache_products(items):
 # Persist only real supplier records in SQLite; never fabricate product data.
 if not items: return
 with DB_LOCK, connect() as db:
  for x in items:
   pid=str(x.get('id') or '').strip(); name=str(x.get('name') or '').strip(); cat=CATEGORY_MAP.get(str(x.get('category') or '').lower(),'')
   if not pid or not name or not cat: continue
   db.execute("""INSERT INTO products(id,category,brand,name,price_krw,image_url,product_url,description,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET category=excluded.category,brand=excluded.brand,name=excluded.name,
    price_krw=CASE WHEN excluded.price_krw>0 THEN excluded.price_krw ELSE products.price_krw END,
    image_url=excluded.image_url,product_url=excluded.product_url,description=excluded.description,updated_at=excluded.updated_at""",
    (pid,cat,str(x.get('brand') or ''),name,price_num(x.get('priceKrw')),valid_url(x.get('imageUrl')),valid_url(x.get('url')),str(x.get('source') or 'Danawa live catalogue'),now_iso()))
  db.commit()

def live_search(q, category, page, page_size):
 # ReefAPI serves up to 40 rows per upstream page. Build the UI page from
 # multiple upstream pages, because provider row counts/has_more can be incomplete.
 defaults = {
  'cpu': 'CPU', 'motherboard': '메인보드', 'ram': '메모리', 'gpu': '그래픽카드',
  'ssd': 'SSD', 'hdd': 'HDD', 'cooler': 'CPU 쿨러', 'case': 'PC 케이스',
  'psu': '파워서플라이', 'software': '소프트웨어', 'cable': '케이블'
 }
 query = (q or defaults.get(category, 'PC 부품')).strip()[:160]
 page = max(1, int(page))
 page_size = max(1, min(40, int(page_size)))
 wanted = page * page_size
 cache_key = (query.casefold(), category or '')

 def source_category(value):
  value = str(value or '').casefold()
  if re.search(r'cpu\s*쿨러|쿨러|냉각|수랭|수냉|cooler', value): return 'cooler'
  if re.search(r'케이스|computer case|\bcase\b', value): return 'case'
  if re.search(r'파워|전원공급장치|power supply|\bpsu\b', value): return 'psu'
  if re.search(r'메인보드|마더보드|motherboard|mainboard', value): return 'motherboard'
  if re.search(r'그래픽카드|비디오카드|\bgpu\b|\bvga\b|graphics card', value): return 'gpu'
  if re.search(r'메모리|램|\bram\b|memory', value): return 'ram'
  if re.search(r'\bssd\b|솔리드스테이트', value): return 'ssd'
  if re.search(r'\bhdd\b|하드디스크|하드 드라이브', value): return 'hdd'
  if re.search(r'프로세서|중앙처리장치|\bcpu\b|processor', value): return 'cpu'
  if re.search(r'소프트웨어|software|케이블|cable', value):
   return 'software' if re.search(r'소프트웨어|software', value) else 'cable'
  return ''

 def find_result_rows(data):
  # Reef's documented field is data.results; also tolerate wrapper/version variants.
  # Pick the list with the largest number of product-shaped records, not just the
  # first arbitrary nested list (which could be variants/specs/metadata).
  best = []
  best_score = 0
  for obj in recursive_dicts(data):
   for key in ('results', 'items', 'products', 'records', 'product_list', 'productList'):
    value = obj.get(key)
    if not isinstance(value, list) or not value:
     continue
    score = 0
    for row in value:
     if not isinstance(row, dict):
      continue
     has_id = any(row.get(k) for k in ('product_id', 'productId', 'pcode', 'id', 'externalId'))
     has_name = any(row.get(k) for k in ('name', 'title', 'product_name', 'productName'))
     if has_id and has_name:
      score += 1
    if score > best_score or (score == best_score and score > 0 and len(value) > len(best)):
     best, best_score = value, score
  return best if best_score else []

 with LIVE_SEARCH_LOCK:
  now = time.time()
  entry = LIVE_SEARCH_CACHE.get(cache_key)
  if not entry or now - entry.get('updated_at', 0) > LIVE_SEARCH_CACHE_TTL:
   entry = {
    'items': [], 'ids': set(), 'raw_ids': set(), 'source_page': 0, 'total': 0,
    'has_more': True, 'updated_at': now, 'last_raw_rows': 0,
    'last_extracted': 0, 'last_added': 0, 'pages_fetched': 0
   }
   LIVE_SEARCH_CACHE[cache_key] = entry

  fetched_this_call = 0
  while (len(entry['items']) < wanted and entry['has_more'] and
         entry['source_page'] < 50 and fetched_this_call < 10):
   upstream_page = entry['source_page'] + 1
   data = reef_post('/danawa/v1/search', {
    'query': query, 'scope': 'catalog', 'page': upstream_page, 'sort': 'popular'
   })
   entry['source_page'] = upstream_page
   fetched_this_call += 1
   entry['pages_fetched'] += 1
   entry['total'] = max(entry['total'], extract_total(data, 0))

   raw_rows = find_result_rows(data)
   items = extract_products(data)
   # If the wrapper hides the row array under an unexpected key, normalized
   # product records still prove that this upstream page returned products.
   if not raw_rows and items:
    raw_rows = items
   entry['last_raw_rows'] = len(raw_rows)
   entry['last_extracted'] = len(items)

   raw_ids = set()
   for row in raw_rows:
    if not isinstance(row, dict):
     continue
    rid = str(row.get('product_id') or row.get('productId') or row.get('pcode') or row.get('id') or row.get('externalId') or '').strip()
    if rid:
     raw_ids.add(rid)
   fresh_raw_ids = raw_ids - entry.get('raw_ids', set())
   entry.setdefault('raw_ids', set()).update(raw_ids)

   added = 0
   raw_categories = {}
   for row in recursive_dicts(data):
    pid = str(row.get('product_id') or row.get('productId') or row.get('pcode') or row.get('id') or '').strip()
    cat_text = row.get('category') or row.get('category_name') or row.get('categoryName') or row.get('category_path') or ''
    if pid and cat_text:
     raw_categories[pid] = str(cat_text)

   for item in items:
    src_cat = source_category(raw_categories.get(str(item.get('externalId') or ''), ''))
    if src_cat:
     item['category'] = src_cat
    if category and item.get('category') not in (category, 'other'):
     continue
    pid = str(item.get('id') or '')
    if not pid or pid in entry['ids']:
     continue
    entry['ids'].add(pid)
    entry['items'].append(item)
    added += 1
   entry['last_added'] = added

   # Do not stop merely because ReefAPI marks has_more=false: observed responses
   # can contain a large catalogue total but only a partial row set. Continue while
   # the provider returns new product IDs, stopping on an empty/repeated page.
   if not raw_rows or (upstream_page > 1 and raw_ids and not fresh_raw_ids):
    entry['has_more'] = False
   else:
    entry['has_more'] = upstream_page < 50
   entry['updated_at'] = time.time()

  start = (page - 1) * page_size
  selected = entry['items'][start:start + page_size]
  total = entry['total'] or len(entry['items'])
  still_has_more = len(entry['items']) > start + page_size or entry['has_more']
  loaded_pages = entry['source_page']
  diagnostics = {
   'upstreamPagesFetchedThisRequest': fetched_this_call,
   'upstreamPageReached': loaded_pages,
   'rawRowsOnLastUpstreamPage': entry.get('last_raw_rows', 0),
   'productsExtractedOnLastUpstreamPage': entry.get('last_extracted', 0),
   'productsAddedOnLastUpstreamPage': entry.get('last_added', 0),
   'cachedFilteredProducts': len(entry['items']),
   'returnedProducts': len(selected)
  }

 cache_products(selected)
 return {
  'items': selected, 'total': total, 'page': page, 'pageSize': page_size,
  'currency': 'KRW', 'updatedAt': now_iso(), 'source': 'Danawa via ReefAPI', 'live': True,
  'note': f'실제 이미지가 확인된 상품만 표시합니다. 공급사 결과를 모아 {page_size}개씩 나눕니다. 공급사 조회 페이지: {loaded_pages}/50.',
  'hasMore': still_has_more, 'diagnostics': diagnostics
 }

def live_product_price(pid):
 # Only Danawa IDs are resolved through the live product endpoint. Cache avoids repeated paid calls during rapid refreshes.
 key=str(pid).replace('danawa:','')
 cached=PRICE_CACHE.get(key)
 if cached and time.time()-cached[0] < PRICE_CACHE_SECONDS: return cached[1]
 data=reef_post('/danawa/v1/product',{'product_id':key})
 row=next((x for x in recursive_dicts(data) if any(k in x for k in ('lowest_price','lowestPrice','price'))),{})
 price=price_num(row.get('lowest_price') or row.get('lowestPrice') or row.get('price'))
 result={'id':key,'externalId':key,'priceKrw':price or None,'currency':'KRW','source':'다나와 가격비교 API','updatedAt':now_iso(),'priceStatus':'live' if price else 'not_available','url':'https://prod.danawa.com/info/?pcode='+key}
 PRICE_CACHE[key]=(time.time(),result); return result

class Handler(BaseHTTPRequestHandler):
 server_version='PCQuoteLiveCatalog/2.0'
 def log_message(self,fmt,*args): sys.stdout.write('[%s] %s\n'%(self.log_date_time_string(),fmt%args))
 def send_json(self,status,payload):
  b=json.dumps(payload,ensure_ascii=False).encode(); self.send_response(status); self.send_header('Content-Type','application/json; charset=utf-8'); self.send_header('Content-Length',str(len(b))); self.send_header('Access-Control-Allow-Origin','*'); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(b)
 def do_OPTIONS(self):
  self.send_response(204); self.send_header('Access-Control-Allow-Origin','*'); self.send_header('Access-Control-Allow-Methods','GET, OPTIONS'); self.send_header('Access-Control-Allow-Headers','Content-Type'); self.end_headers()
 def do_GET(self):
  u=urlparse(self.path); p=parse_qs(u.query)
  try:
   if u.path=='/health': self.send_json(200,{'ok':True,'products':count_products(),'liveProviderConfigured':bool(REEF_API_KEY),'provider':'Danawa via ReefAPI' if REEF_API_KEY else None,'priceCacheSeconds':PRICE_CACHE_SECONDS,'updatedAt':now_iso()}); return
   if u.path=='/api/catalog/search':
    q=(p.get('q',[''])[0] or '').strip()[:160]; cat=CATEGORY_MAP.get((p.get('category',[''])[0] or '').strip().lower(),''); page=max(1,min(50,int(p.get('page',['1'])[0] or '1'))); size=max(1,min(40,int(p.get('pageSize',['24'])[0] or '24')))
    if REEF_API_KEY:
     if not q: q={'ram':'DDR5 메모리','ssd':'SSD','cpu':'CPU','gpu':'그래픽카드','motherboard':'메인보드','hdd':'HDD','cooler':'CPU 쿨러','case':'PC 케이스','psu':'파워서플라이'}.get(cat,'PC 부품')
     self.send_json(200,live_search(q,cat,page,size)); return
    self.send_json(503,{'error':'live_provider_not_configured','message':'실시간 상품 검색을 사용하려면 서버에 REEF_API_KEY를 설정해야 합니다. 임의 가격은 반환하지 않습니다.','items':[],'total':0}); return
   if u.path=='/api/parts/prices':
    ids=[x for x in (p.get('ids',[''])[0] or '').split(',') if x][:20]
    if REEF_API_KEY and ids:
     items=[]; errors=[]
     for pid in ids:
      try: items.append(live_product_price(pid))
      except Exception as e: errors.append({'id':pid,'error':str(e)})
     self.send_json(200,{'items':items,'updatedAt':now_iso(),'live':True,'errors':errors,'message':'공급사 API 응답에 가격이 있을 때만 가격을 표시합니다.'}); return
    self.send_json(503 if not REEF_API_KEY else 200,{'items':[{'id':'__connection_test__','priceKrw':None,'currency':'KRW'}] if REEF_API_KEY and not ids else [],'updatedAt':now_iso(),'live':False,'priceFeedConfigured':bool(REEF_API_KEY),'message':'실제 가격을 가져올 상품 ID 또는 공급사 API 키가 없습니다. 가격을 임의 생성하지 않았습니다.'}); return
   self.send_json(404,{'error':'not_found','message':'지원하지 않는 경로입니다.'})
  except Exception as e:
   self.send_json(502,{'error':'provider_error','message':str(e),'items':[],'live':False})

def main():
 global DB_PATH
 ap=argparse.ArgumentParser(); ap.add_argument('--import',dest='csv_path'); ap.add_argument('--import-only',action='store_true'); ap.add_argument('--db',default=str(DB_PATH)); ap.add_argument('--host',default=os.environ.get('HOST','0.0.0.0')); ap.add_argument('--port',default=int(os.environ.get('PORT','8787')),type=int); a=ap.parse_args()
 DB_PATH=Path(a.db).resolve(); init_db()
 if a.csv_path: import_catalog_csv(Path(a.csv_path).resolve())
 if a.import_only: return
 print('PC 견적 API 서버 시작'); print(f'DB: {DB_PATH}'); print(f'상품 수(로컬 DB): {count_products():,}'); print(f'실시간 다나와 API: {"설정됨" if REEF_API_KEY else "미설정 - 임의 가격 미사용"}'); print(f'포트: {a.port}')
 ThreadingHTTPServer((a.host,a.port),Handler).serve_forever()
if __name__=='__main__': main()
