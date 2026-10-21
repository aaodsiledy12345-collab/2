#!/usr/bin/env python3
"""PC quote API: licensed CSV catalogue + optional live Danawa data via ReefAPI.
No synthetic/default prices are generated. Provider credentials remain server-side.
"""
from __future__ import annotations
import argparse, csv, json, os, re, sqlite3, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
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
SERVER_BUILD = 'pc-catalog-category-speed-fix-2026-10-22-b'
# Cache normalized search results so UI pages of 24 products can be assembled
# from ReefAPI's 40-row upstream pages without skipping products between pages.
LIVE_SEARCH_CACHE = {}
LIVE_SEARCH_LOCK = threading.RLock()
LIVE_SEARCH_KEY_LOCKS = {}
LIVE_SEARCH_CACHE_TTL = max(60, PRICE_CACHE_SECONDS)

CATEGORY_MAP = {
 'cpu':'cpu','processor':'cpu','프로세서':'cpu','중앙처리장치':'cpu',
 'motherboard':'motherboard','mainboard':'motherboard','메인보드':'motherboard', 'board':'motherboard', 'm/b':'motherboard',
 'ram':'ram','memory':'ram','메모리':'ram','gpu':'gpu','vga':'gpu','graphics':'gpu','그래픽카드':'gpu',
 'ssd':'ssd','hdd':'hdd','cooler':'cooler','cooling':'cooler','쿨러':'cooler','튜닝':'cooler','쿨러/튜닝':'cooler','cooler/tuning':'cooler','cooling/tuning':'cooler','tuning':'cooler','fan':'cooler',
 'case':'case','케이스':'case','pc case':'case','pccase':'case','psu':'psu','power':'psu','power supply':'psu','powersupply':'psu','파워':'psu','파워서플라이':'psu','software':'software','소프트웨어':'software','os':'software','operating system':'software','s/w':'software','cable':'cable','cables':'cable','케이블':'cable','accessory cable':'cable','cable/adapter':'cable'
}

# Danawa category IDs confirmed from product/category URLs. The API's optional
# `category` filter is essential: querying only a broad word can return unrelated
# rows and a misleading site-wide total. Software and general cable categories
# use targeted keywords until a single stable parent category is confirmed.
DANAWA_CATEGORY_IDS = {
    'cpu': '112747',
    'cooler': '11353457',
    'motherboard': '11345282',
    'ram': '11341201',
    'gpu': '11347368',
    'ssd': '11335282',
    'hdd': '1131401',
    'case': '11312471',
    'psu': '1131496',
}
CATEGORY_DEFAULT_QUERY = {
    'cpu': 'CPU', 'motherboard': '메인보드', 'ram': 'DDR5 메모리',
    'gpu': '그래픽카드', 'ssd': 'SSD', 'hdd': 'HDD 하드디스크',
    'cooler': 'CPU 쿨러', 'case': 'PC 케이스', 'psu': '파워서플라이',
    'software': '소프트웨어', 'cable': '케이블',
}
CATEGORY_FALLBACK_QUERIES = {
    'cpu': ['인텔 CPU', 'AMD 라이젠 CPU', '코어 울트라 프로세서'],
    'motherboard': ['AMD 메인보드', '인텔 메인보드', 'B650 메인보드'],
    'ram': ['DDR4 메모리', '삼성전자 DDR5 메모리', 'SK하이닉스 메모리'],
    'gpu': ['RTX 그래픽카드', 'GeForce 그래픽카드', 'Radeon 그래픽카드'],
    'ssd': ['NVMe SSD', 'M.2 SSD', 'SATA SSD'],
    'hdd': ['씨게이트 HDD', 'Western Digital 하드디스크', 'NAS HDD'],
    'cooler': ['공랭 CPU 쿨러', '수랭 CPU 쿨러', '케이스 팬 쿨러'],
    'case': ['미들타워 PC 케이스', '빅타워 케이스', '미니타워 케이스'],
    'psu': ['ATX 파워서플라이', '750W 파워', '컴퓨터 파워'],
    'software': ['Windows 11 정품', 'Microsoft Office 정품', '한컴오피스 정품'],
    'cable': ['USB 케이블', 'DP 케이블', 'SATA 케이블'],
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
 with urlopen(req,timeout=10) as r: body=json.loads(r.read().decode('utf-8'))
 if isinstance(body,dict) and body.get('ok') is False: raise RuntimeError(str(body.get('error') or '가격 공급사 API 오류'))
 return body.get('data',body) if isinstance(body,dict) else body

def recursive_dicts(obj):
 if isinstance(obj,dict):
  yield obj
  for v in obj.values(): yield from recursive_dicts(v)
 elif isinstance(obj,list):
  for v in obj: yield from recursive_dicts(v)

def result_rows(data):
 # ReefAPI's documented search payload is data.results (40 rows/page).
 # Fall back to alternate wrappers, choosing the list with most product-shaped rows.
 if isinstance(data, dict):
  for key in ("results", "items", "products", "records", "product_list", "productList"):
   value = data.get(key)
   if isinstance(value, list) and value:
    return value
 best, score = [], 0
 for obj in recursive_dicts(data):
  for key in ("results", "items", "products", "records", "product_list", "productList"):
   value = obj.get(key)
   if not isinstance(value, list) or not value:
    continue
   n = sum(1 for r in value if isinstance(r, dict) and
           any(r.get(k) for k in ("product_id", "productId", "pcode", "id", "externalId")) and
           any(r.get(k) for k in ("name", "title", "product_name", "productName")))
   if n > score or (n == score and n > 0 and len(value) > len(best)):
    best, score = value, n
 return best

def classify_category(name, category_text="", fallback=""):
 # Trust the actual Danawa category path first. The product title often contains
 # only a model/brand (e.g. "Cooler Master") and is not a reliable category.
 cat_text = str(category_text or "").casefold()
 tests = [
  ("motherboard", r"메인보드|마더보드|motherboard|mainboard"),
  ("hdd", r"\bhdd\b|하드디스크|하드 드라이브|hard.?disk"),
  ("ssd", r"\bssd\b|nvme|솔리드.?스테이트"),
  ("ram", r"메모리|램|\bram\b|\bddr[345]\b|memory"),
  ("cpu", r"프로세서|중앙처리장치|\bcpu\b|processor"),
  ("gpu", r"그래픽카드|비디오카드|\bgpu\b|\bvga\b|graphics card|geforce|\brtx\s*\d|\bgtx\s*\d|라데온|radeon"),
  ("cooler", r"cpu.?쿨러|쿨러|냉각|수랭|수냉|라디에이터|cooling"),
  ("case", r"pc.?케이스|컴퓨터.?케이스|미들타워|빅타워|케이스|computer case"),
  ("psu", r"파워서플라이|전원공급장치|power supply|\bpsu\b"),
  ("software", r"소프트웨어|운영체제|\bwindows\b|윈도우|microsoft office|\boffice\b|software|한글과컴퓨터"),
  ("cable", r"케이블|젠더|연장선|hdmi|displayport|\busb[- ]?c\b|sata.?케이블|cable|adapter"),
 ]
 for category, pattern in tests:
  if cat_text and re.search(pattern, cat_text, re.I):
   return category
 # The category tab/search itself is the best source when Danawa omits a useful
 # category path. This also prevents model names from being misfiled (e.g. Cooler
 # Master case/PSU models incorrectly classified as CPU coolers).
 if fallback:
  return fallback
 # Only when no category was selected, infer from the title.
 title = str(name or "").casefold()
 for category, pattern in tests:
  if re.search(pattern, title, re.I):
   return category
 return "other"

def _as_image_url(value):
 if isinstance(value, str):
  return valid_url(value)
 if isinstance(value, dict):
  for k in ("url", "src", "image_url", "imageUrl", "img_url", "image_link", "imageLink", "thumbnail", "large", "original", "main"):
   u = valid_url(value.get(k))
   if u:
    return u
 if isinstance(value, list):
  for item in value:
   u = _as_image_url(item)
   if u:
    return u
 return ""

def extract_products(data, fallback_category=""):
 # Normalize the actual result rows, rather than recursively mistaking nested
 # variant/spec dictionaries for separate products. Keep only genuine image URLs.
 candidates = []
 rows = result_rows(data)
 if not rows:
  rows = list(recursive_dicts(data))
 for x in rows:
  if not isinstance(x, dict):
   continue
  row_type = str(x.get("row_type") or x.get("rowType") or "product").casefold()
  if row_type not in ("product", ""):
   continue
  name = str(x.get("name") or x.get("product_name") or x.get("title") or x.get("productName") or "").strip()
  pid = str(x.get("product_id") or x.get("productId") or x.get("pcode") or x.get("id") or x.get("externalId") or "").strip()
  if not name or not pid:
   continue
  img = ""
  for k in ("image_url", "imageUrl", "image", "thumbnail", "img", "images", "thumbnail_url", "thumbnailUrl", "product_image", "productImage", "product_img", "img_url", "imgUrl", "image_link", "imageLink", "main_image", "mainImage", "photo", "photos"):
   img = _as_image_url(x.get(k))
   if img:
    break
  if not img:
   continue
  url = valid_url(x.get("url") or x.get("product_url") or x.get("productUrl"))
  if not url and pid.isdigit():
   url = "https://prod.danawa.com/info/?pcode=" + pid
  price = price_num(x.get("lowest_price") or x.get("price") or x.get("lowestPrice") or x.get("price_krw"))
  brand = str(x.get("maker") or x.get("brand") or x.get("manufacturer") or "").strip()
  category_text = " ".join(str(x.get(k) or "") for k in ("category", "category_name", "categoryName", "category_path", "categoryPath"))
  category = classify_category(name, category_text, fallback_category)
  candidates.append({
   "id": "danawa:" + pid, "externalId": pid, "category": category,
   "brand": brand, "name": name, "priceKrw": price or None, "currency": "KRW",
   "imageUrl": img, "url": url, "source": "다나와 가격비교 API",
   "updatedAt": now_iso(), "priceStatus": "live" if price else "not_available"
  })
 seen, out = set(), []
 for item in candidates:
  if item["id"] not in seen:
   seen.add(item["id"])
   out.append(item)
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
 # Use Danawa's real category filter where we have a verified category ID.
 # ReefAPI documents page (1-based) and 40 rows/page; page_size/limit are not
 # accepted parameters on this endpoint. Cache upstream rows so web pages of 24
 # items can be assembled from one or occasionally a few upstream requests.
 category = CATEGORY_MAP.get(str(category or '').strip().lower(), str(category or '').strip().lower())
 category = category if category in set(CATEGORY_DEFAULT_QUERY) else ''
 query = (q or '').strip()[:160]
 generic_terms = {
  'cpu','processor','프로세서','중앙처리장치','메인보드','motherboard','mainboard','m/b',
  'ram','memory','메모리','gpu','vga','그래픽카드','ssd','hdd','하드디스크',
  'cooler','쿨러','튜닝','쿨러/튜닝','cooler/tuning','case','케이스','pc case',
  'psu','power','power supply','파워','파워서플라이','software','소프트웨어',
  'cable','cables','케이블','pc 부품','pc parts'
 }
 is_category_browse = (not query) or (bool(category) and query.casefold() in generic_terms)
 if is_category_browse:
  query = CATEGORY_DEFAULT_QUERY.get(category, 'PC 부품')

 page = max(1, int(page))
 page_size = max(1, min(40, int(page_size)))
 wanted = page * page_size
 category_id = DANAWA_CATEGORY_IDS.get(category)
 cache_key = (query.casefold(), category or '', category_id or '')
 with LIVE_SEARCH_LOCK:
  request_lock = LIVE_SEARCH_KEY_LOCKS.setdefault(cache_key, threading.RLock())

 with request_lock:
  now = time.time()
  entry = LIVE_SEARCH_CACHE.get(cache_key)
  if not entry or now - entry.get('updated_at', 0) > LIVE_SEARCH_CACHE_TTL:
   entry = {
    'query': query, 'category_id': category_id, 'items': [], 'ids': set(),
    'source_page': 0, 'total': 0, 'has_more': True, 'updated_at': now,
    'raw_rows': 0, 'normalized': 0, 'last_added': 0, 'pages_fetched': 0,
    'page_signatures': set(), 'errors': [], 'fallback_used': False,
   }
   LIVE_SEARCH_CACHE[cache_key] = entry

  fetched_this_call = 0
  max_fetches_this_call = 6

  def fetch_source_page(source_page, q_override=None, use_category=True):
   actual_query = (q_override or entry['query']).strip()
   payload = {'query': actual_query, 'scope': 'catalog', 'page': source_page}
   cat_id = entry.get('category_id') if use_category else None
   if cat_id:
    payload['category'] = cat_id
   data = reef_post('/danawa/v1/search', payload)
   return actual_query, cat_id, data

  def consume_source_page(upstream_page, data, actual_query=None, cat_id=None):
   if not isinstance(data, dict):
    data = {'results': data if isinstance(data, list) else []}
   entry['source_page'] = upstream_page
   entry['pages_fetched'] += 1
   # Prefer a query result total when present. Keep the provider count unchanged;
   # it is not a promise that every row has an image usable by this UI.
   current_total = data.get('total_results') or data.get('totalResults') or data.get('total') or data.get('catalog_total') or data.get('total_count') or 0
   try:
    entry['total'] = max(entry['total'], int(current_total))
   except (ValueError, TypeError):
    pass
   raw_rows = result_rows(data)
   entry['raw_rows'] = len(raw_rows)
   items = extract_products(data, fallback_category=category)
   # This is a category-scoped UI. The provider category filter (when available)
   # and category-specific query are the source of truth for the requested tab.
   if category:
    for item in items:
     item['category'] = category
   entry['normalized'] = len(items)
   signature = tuple(str(x.get('externalId') or x.get('id') or '') for x in items)
   if signature and signature in entry['page_signatures']:
    entry['has_more'] = False
    entry['last_added'] = 0
    print(f"[catalog-search] repeated page={upstream_page} query={actual_query!r}; stopping", flush=True)
    return False
   if signature:
    entry['page_signatures'].add(signature)
   added = 0
   for item in items:
    pid = str(item.get('id') or '')
    if not pid or pid in entry['ids']:
     continue
    entry['ids'].add(pid)
    entry['items'].append(item)
    added += 1
   entry['last_added'] = added
   entry['updated_at'] = time.time()

   documented_total = entry['total']
   source_has_more = data.get('has_more')
   # ReefAPI says page 50 repeats page 50; therefore also detect repeated IDs.
   # Ignore a premature false has_more when a larger result count remains.
   if not raw_rows:
    entry['has_more'] = False
   elif source_has_more is False and documented_total and documented_total <= upstream_page * 40:
    entry['has_more'] = False
   else:
    entry['has_more'] = upstream_page < 50
   print(
    f"[catalog-search] build={SERVER_BUILD} category={category or 'all'} "
    f"query={actual_query!r} upstream_page={upstream_page} category_id={cat_id or '-'} "
    f"raw={len(raw_rows)} normalized={len(items)} added={added} total={documented_total}",
    flush=True,
   )
   return True

  # First source page: retry without the category filter if the official category
  # ID was rejected or returned no visible products. This specifically prevents
  # a bad/missing category mapping from leaving a tab permanently empty.
  if entry['source_page'] == 0 and fetched_this_call < max_fetches_this_call:
   try:
    actual_query, cat_id, data = fetch_source_page(1, use_category=True)
    fetched_this_call += 1
    consume_source_page(1, data, actual_query, cat_id)
   except Exception as e:
    entry['errors'].append(str(e)[:240])
    print(f"[catalog-search] first request failed with category filter: {e}", flush=True)
    try:
     actual_query, cat_id, data = fetch_source_page(1, use_category=False)
     fetched_this_call += 1
     entry['query'] = actual_query
     entry['category_id'] = None
     consume_source_page(1, data, actual_query, None)
     entry['fallback_used'] = True
    except Exception as e2:
     entry['errors'].append(str(e2)[:240])
     entry['has_more'] = False

   # If the category-filtered search returns no image-backed products, retry
   # with keyword-only category synonyms. Only then do we fan out to more pages.
   if not entry['items'] and is_category_browse and category and fetched_this_call < max_fetches_this_call:
    if entry.get('source_page') == 1:
     entry['source_page'] = 0
     entry['has_more'] = True
    entry['page_signatures'] = set()
    entry['items'] = []
    entry['ids'] = set()
    entry['total'] = 0
    for alt_query in CATEGORY_FALLBACK_QUERIES.get(category, []):
     if fetched_this_call >= max_fetches_this_call or entry['items']:
      break
     try:
      actual_query, _, alt_data = fetch_source_page(1, q_override=alt_query, use_category=False)
      fetched_this_call += 1
      # A fallback search becomes the active stream so that following pages
      # continue the same query rather than mixing page indices from searches.
      entry['query'] = alt_query
      entry['category_id'] = None
      entry['source_page'] = 0
      entry['page_signatures'] = set()
      consume_source_page(1, alt_data, actual_query, None)
      entry['fallback_used'] = True
     except Exception as e:
      entry['errors'].append(str(e)[:240])
      print(f"[catalog-search] fallback query failed query={alt_query!r}: {e}", flush=True)

  # Usually the first ReefAPI page contains 40 rows, so a 24-row UI page needs
  # one call. If images/deduplication leave fewer rows, collect more source pages
  # in parallel (bounded at six calls per HTTP request to limit latency and cost).
  while len(entry['items']) < wanted and entry['has_more'] and entry['source_page'] < 50 and fetched_this_call < max_fetches_this_call:
   deficit = wanted - len(entry['items'])
   estimated_yield = max(1, int(entry.get('last_added', 0)))
   pages_needed = max(1, (deficit + estimated_yield - 1) // estimated_yield)
   budget = max_fetches_this_call - fetched_this_call
   count = min(budget, pages_needed, 50 - entry['source_page'])
   page_numbers = list(range(entry['source_page'] + 1, entry['source_page'] + count + 1))
   if not page_numbers:
    break
   outcomes = []
   with ThreadPoolExecutor(max_workers=min(6, len(page_numbers))) as pool:
    futures = [(n, pool.submit(fetch_source_page, n, use_category=bool(entry.get('category_id')))) for n in page_numbers]
    for n, future in futures:
     try:
      outcomes.append((n, future.result(), None))
     except Exception as e:
      outcomes.append((n, None, str(e)))
   fetched_this_call += len(outcomes)
   consecutive = []
   # Process in page order. Do not silently skip a failed page number.
   for n, outcome, err in outcomes:
    if err:
     entry['errors'].append(err[:240])
     print(f"[catalog-search] page={n} failed: {err}", flush=True)
     break
    actual_query, cat_id, data = outcome
    consecutive.append((n, data, actual_query, cat_id))
   for n, data, actual_query, cat_id in consecutive:
    if n != entry['source_page'] + 1:
     break
    if not consume_source_page(n, data, actual_query, cat_id):
     break
    if len(entry['items']) >= wanted or not entry['has_more']:
     break
   if not consecutive:
    break

  start = (page - 1) * page_size
  selected = entry['items'][start:start + page_size]
  total = entry['total'] or len(entry['items'])
  still_has_more = len(entry['items']) > start + page_size or entry['has_more']
  diagnostics = {
   'requestedCategory': category or None,
   'providerCategoryId': entry.get('category_id'),
   'queryUsed': entry.get('query'),
   'fallbackUsed': bool(entry.get('fallback_used')),
   'upstreamPagesFetchedThisRequest': fetched_this_call,
   'upstreamPageReached': entry['source_page'],
   'rawRowsOnLastUpstreamPage': entry.get('raw_rows', 0),
   'productsNormalizedOnLastUpstreamPage': entry.get('normalized', 0),
   'productsAddedOnLastUpstreamPage': entry.get('last_added', 0),
   'cachedFilteredProducts': len(entry['items']),
   'returnedProducts': len(selected),
   'errors': entry.get('errors', [])[-3:],
   'maxUpstreamCallsPerRequest': max_fetches_this_call,
  }
  cache_products(selected)
  return {
   'items': selected, 'total': total, 'page': page, 'pageSize': page_size,
   'currency': 'KRW', 'updatedAt': now_iso(), 'source': 'Danawa via ReefAPI',
   'live': True, 'serverBuild': SERVER_BUILD,
   'note': f"실제 이미지가 있는 상품만 표시합니다. 공급사 페이지 {entry['source_page']}/50에서 {len(selected)}개 반환.",
   'hasMore': still_has_more, 'diagnostics': diagnostics,
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
 server_version='PCQuoteLiveCatalog/2.1'
 def log_message(self,fmt,*args): sys.stdout.write('[%s] %s\n'%(self.log_date_time_string(),fmt%args))
 def send_json(self,status,payload):
  b=json.dumps(payload,ensure_ascii=False).encode(); self.send_response(status); self.send_header('Content-Type','application/json; charset=utf-8'); self.send_header('Content-Length',str(len(b))); self.send_header('Access-Control-Allow-Origin','*'); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(b)
 def do_OPTIONS(self):
  self.send_response(204); self.send_header('Access-Control-Allow-Origin','*'); self.send_header('Access-Control-Allow-Methods','GET, OPTIONS'); self.send_header('Access-Control-Allow-Headers','Content-Type'); self.end_headers()
 def do_GET(self):
  u=urlparse(self.path); p=parse_qs(u.query)
  try:
   if u.path=='/health': self.send_json(200,{'ok':True,'serverBuild':SERVER_BUILD,'products':count_products(),'liveProviderConfigured':bool(REEF_API_KEY),'provider':'Danawa via ReefAPI' if REEF_API_KEY else None,'priceCacheSeconds':PRICE_CACHE_SECONDS,'updatedAt':now_iso()}); return
   if u.path=='/api/catalog/search':
    q=(p.get('q',[''])[0] or '').strip()[:160]; cat=CATEGORY_MAP.get((p.get('category',[''])[0] or '').strip().lower(),''); page=max(1,min(50,int(p.get('page',['1'])[0] or '1'))); size=max(1,min(40,int(p.get('pageSize',['24'])[0] or '24')))
    if REEF_API_KEY:
     # live_search chooses a correct category-specific query, including HDD,
     # cooler/tuning, software and cables. Do not replace blanks with generic
     # 'PC 부품' here, because that bypasses those defaults.
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
