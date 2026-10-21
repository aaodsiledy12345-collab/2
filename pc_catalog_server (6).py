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
SERVER_BUILD = 'pc-catalog-taxonomy-cache-speed-taxonomy-2026-10-22-f'
# Cache normalized search results so UI pages of 24 products can be assembled
# from ReefAPI's 40-row upstream pages without skipping products between pages.
LIVE_SEARCH_CACHE = {}
LIVE_SEARCH_LOCK = threading.RLock()
LIVE_SEARCH_KEY_LOCKS = {}
LIVE_SEARCH_CACHE_TTL = max(300, PRICE_CACHE_SECONDS)
PREFETCH_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix='catalog-prefetch')
PREFETCH_LOCK = threading.Lock()
PREFETCH_IN_FLIGHT = set()

CATEGORY_MAP = {
 'cpu':'cpu','processor':'cpu','프로세서':'cpu','중앙처리장치':'cpu',
 'motherboard':'motherboard','mainboard':'motherboard','메인보드':'motherboard', 'board':'motherboard', 'm/b':'motherboard',
 'ram':'ram','memory':'ram','메모리':'ram','gpu':'gpu','vga':'gpu','graphics':'gpu','그래픽카드':'gpu',
 'ssd':'ssd','hdd':'hdd','cooler':'cooler','cooling':'cooler','쿨러':'cooler','튜닝':'cooler','쿨러/튜닝':'cooler','cooler/tuning':'cooler','cooling/tuning':'cooler','tuning':'cooler','fan':'cooler',
 'case':'case','케이스':'case','pc case':'case','pccase':'case','psu':'psu','power':'psu','power supply':'psu','powersupply':'psu','파워':'psu','파워서플라이':'psu','software':'software','소프트웨어':'software','os':'software','operating system':'software','s/w':'software','cable':'cable','cables':'cable','케이블':'cable','accessory cable':'cable','cable/adapter':'cable'
}

# ReefAPI accepts a Danawa category filter. Only use IDs backed by current
# product/category URLs; for categories with many separate subtrees, use keywords.
# Several categories
# have multiple subtrees (notably CPU, HDD, cooler, software and cable), so for
# those we search focused keywords and strictly reject clearly mismatched titles.
DANAWA_CATEGORY_IDS = {
    'cpu': '112747',          # CPU
    'motherboard': '112751',  # motherboard
    'ram': '112752',          # memory
    'gpu': '112753',          # graphics cards
    'ssd': '112760',          # SSD
    'hdd': '112763',          # HDD
    'case': '112775',         # PC case
    'psu': '112777',          # PC power supply
}
# Verified Danawa leaf/category IDs used as separate streams. Some components
# sit in several different category trees, so a single keyword is not enough.
DANAWA_EXTRA_CATEGORY_STREAMS = {
    'hdd': [('데스크탑 HDD', '21349947'), ('노트북 HDD', '1131418')],
    'cooler': [('수랭 CPU 쿨러', '11336856'), ('공랭 CPU 쿨러', '11336857')],
    'software': [('Windows 소프트웨어', '1121015'), ('Office 정품', '1136168')],
    'cable': [('USB 케이블', '12356315'), ('USB4 케이블', '11341313')],
}
DANAWA_CATEGORY_ID_MAP = {
    '112747': 'cpu', '112751': 'motherboard', '112752': 'ram', '112753': 'gpu',
    '112760': 'ssd', '112763': 'hdd', '112775': 'case', '112777': 'psu',
    '21349947': 'hdd', '1131418': 'hdd', '11336856': 'cooler',
    '11336857': 'cooler', '1121015': 'software', '1136168': 'software',
    '12356315': 'cable', '11341313': 'cable',
}
CATEGORY_DEFAULT_QUERY = {
    'cpu': 'CPU',
    'motherboard': '메인보드',
    'ram': 'DDR5 메모리',
    'gpu': '그래픽카드',
    'ssd': 'NVMe SSD',
    'hdd': 'HDD 하드디스크',
    'cooler': 'CPU 쿨러',
    'case': 'PC 케이스',
    'psu': '파워서플라이',
    'software': 'Windows 11 정품',
    'cable': 'USB 케이블',
}
# When browsing a category tab, multiple distinct real searches are merged and
# deduplicated. These are query streams, not fabricated records or prices.
CATEGORY_FALLBACK_QUERIES = {
    'cpu': ['AMD Ryzen CPU', 'Intel Core CPU', '코어 울트라 프로세서', 'AMD 라이젠 프로세서', 'Xeon CPU', 'Threadripper CPU'],
    'motherboard': ['AM5 메인보드', '인텔 메인보드', 'B650 메인보드', 'B850 메인보드', 'Z890 메인보드', 'LGA1851 메인보드'],
    'ram': ['DDR4 메모리', '삼성전자 DDR5 메모리', '데스크탑 RAM', '노트북 메모리', 'ECC 메모리', 'SO-DIMM 메모리'],
    'gpu': ['RTX 그래픽카드', 'GeForce 그래픽카드', 'Radeon 그래픽카드', 'RTX 50 그래픽카드', '워크스테이션 그래픽카드', '그래픽카드 16GB'],
    'ssd': ['M.2 NVMe SSD', 'SATA SSD', 'PCIe 5.0 SSD', 'SSD 1TB', 'SSD 2TB', '외장 SSD'],
    'hdd': ['3.5인치 HDD', '2.5인치 HDD', 'NAS HDD', '씨게이트 하드디스크', 'WD HDD', 'PC용 HDD'],
    'cooler': ['공랭 CPU 쿨러', '수랭 CPU 쿨러', '시스템 쿨러 팬', '케이스 팬 쿨러', '라디에이터 쿨러', '일체형 수냉쿨러'],
    'case': ['ATX 케이스', '미들타워 PC 케이스', '미니타워 PC 케이스', '빅타워 케이스', 'ITX 케이스', '컴퓨터 본체 케이스'],
    'psu': ['ATX 파워서플라이', '750W 파워', '850W 파워', '1000W 파워', 'SFX 파워서플라이', '80PLUS 파워', '마이크로닉스 파워서플라이', '시소닉 파워서플라이', 'FSP 파워서플라이'],
    'software': ['Windows 11 Home 정품', 'Windows 11 Pro 정품', 'Windows 10 정품', 'Microsoft 365 정품', 'Office 2024 정품', 'MS Office 정품', '한컴오피스 정품', '백신 소프트웨어 라이선스'],
    'cable': ['HDMI 2.1 케이블', 'DisplayPort 케이블', 'DP 케이블', 'SATA 데이터 케이블', 'USB-C 케이블', 'PC 전원 케이블', '랜케이블', 'PCIe 전원 케이블'],
}
CATEGORY_GENERIC_TERMS = {
    'cpu','processor','프로세서','중앙처리장치', 'motherboard','mainboard','m/b','메인보드',
    'ram','memory','메모리','gpu','vga','그래픽카드','ssd','hdd','하드디스크',
    'cooler','쿨러','튜닝','쿨러/튜닝','cooler/tuning','case','케이스','pc case',
    'psu','power','power supply','파워','파워서플라이','software','소프트웨어','os','운영체제',
    'cable','cables','케이블','pc 부품','pc parts','pc component','pc components'
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

def _product_shaped(row):
    return (isinstance(row, dict)
            and any(row.get(k) for k in ('product_id','productId','pcode','id','externalId'))
            and any(row.get(k) for k in ('name','title','product_name','productName')))

def result_rows(data):
    """Find the actual product array in ReefAPI's {ok,data,meta,error} envelope.

    Do not immediately return the first list named ``items``: wrapper objects can
    include a small summary list alongside a larger results array.
    """
    if isinstance(data, list):
        return data
    keys = ('results','items','products','records','product_list','productList','catalog')
    candidates = []
    for obj in recursive_dicts(data):
        for key in keys:
            value = obj.get(key)
            if not isinstance(value, list) or not value:
                continue
            shaped = sum(1 for row in value if _product_shaped(row))
            if shaped:
                candidates.append((shaped, len(value), value))
    if not candidates:
        return []
    # Most product records wins; length is a tie breaker.
    candidates.sort(key=lambda t: (t[0], t[1]), reverse=True)
    return candidates[0][2]

def _category_from_text_segment(segment):
    text = str(segment or '').strip().casefold().strip(' "\'[]{}')
    if not text or re.fullmatch(r'[\d\s|>/,-]+', text):
        return ''
    aliases = {
        'cpu':'cpu','processor':'cpu','motherboard':'motherboard','mainboard':'motherboard',
        'ram':'ram','memory':'ram','gpu':'gpu','vga':'gpu','ssd':'ssd','hdd':'hdd',
        'cooler':'cooler','cooling':'cooler','tuning':'cooler','fan':'cooler',
        'case':'case','pc case':'case','computer case':'case','psu':'psu','power':'psu',
        'power supply':'psu','software':'software','operating system':'software','os':'software',
        'cable':'cable','cables':'cable','accessory cable':'cable','cable/adapter':'cable',
    }
    if text in aliases:
        return aliases[text]
    rules = [
        ('motherboard', r'메인보드|마더보드|motherboard|mainboard'),
        ('hdd', r'\bhdd\b|하드디스크|하드 드라이브|hard.?disk'),
        ('ssd', r'\bssd\b|nvme|솔리드.?스테이트'),
        ('ram', r'데스크탑 메모리|노트북 메모리|메모리|램|\bram\b|\bddr[345]\b|memory'),
        ('gpu', r'그래픽카드|비디오카드|\bgpu\b|\bvga\b|graphics card|geforce|\brtx\s*\d|\bgtx\s*\d|라데온|radeon'),
        ('cooler', r'cpu.?쿨러|쿨러/튜닝|쿨러|수랭|수냉|공랭|라디에이터|cooling|liquid cooler|water cooler|시스템 팬|튜닝'),
        ('case', r'pc.?케이스|컴퓨터.?케이스|미들타워|빅타워|미니타워|케이스|computer case|chassis'),
        ('psu', r'파워서플라이|전원공급장치|power supply|\bpsu\b'),
        ('software', r'소프트웨어|운영체제|\bwindows\b|윈도우|microsoft\s*(office|365)|\boffice\b|software|한컴오피스'),
        ('cable', r'케이블|젠더|연장선|hdmi|displayport|usb[- ]?c|sata.?케이블|\bcable\b|adapter'),
        ('cpu', r'프로세서|중앙처리장치|\bcpu\b|processor'),
    ]
    for category, pattern in rules:
        if re.search(pattern, text, re.I):
            return category
    return ''

def _category_from_text(text):
    """Parse breadcrumbs and keep the most-specific/rightmost taxonomy node."""
    if not text:
        return ''
    raw = str(text).strip()
    parts = re.split(r'[>›»→|,;]+', raw)
    for part in reversed(parts):
        category = _category_from_text_segment(part)
        if category:
            return category
    return _category_from_text_segment(raw)

def _category_from_title(name):
    title = str(name or '').strip().casefold()
    if not title:
        return ''
    # Remove unrelated home appliances seen in broad searches.
    if re.search(r'캐리어|\bcarrier\b|에어컨|냉난방기|냉장고|세탁기|청소기|안마의자|스마트폰 요금제|번호이동', title, re.I):
        return 'unrelated'
    # Software has distinctive names. Classify before generic interface terms like USB.
    if re.search(r'\bwindows?\b|윈도우\s*\d|microsoft\s*(office|365)|ms office|\boffice\s*(20\d\d|365)|한컴오피스|운영체제|소프트웨어|백신.*(라이선스|정품)|software license|antivirus license', title, re.I):
        return 'software'
    # A recognizable processor SKU beats incidental phrases such as "기본쿨러 포함".
    # Known standalone cooler families are excluded from this shortcut.
    standalone_cooler = re.search(r'cpu.?쿨러|쿨러/튜닝|수랭|수냉|공랭|라디에이터|liquid cooler|water cooler|liquid freezer|kraken\s*[0-9]|masterliquid|galahad|frostflow|peerless assassin|phantom spirit|dark rock|pure rock|hyper\s*\d|nh-u\d|ak\d{2,3}|ls\d{3}', title, re.I)
    processor_sku = re.search(r'라이젠|ryzen|코어 울트라|core\s*ultra|core\s*i[3579]|코어\s*i[3579]|xeon|제온|threadripper|스레드리퍼|\bi[3579]-?\d{4,5}[a-z]*\b', title, re.I)
    if processor_sku and not standalone_cooler:
        return 'cpu'
    if re.search(r'파워서플라이|전원공급장치|power supply|\bpsu\b|80\s*plus|80plus|\b(corsair|seasonic|fsp|super\s*flower|antec|enermax|thermaltake|silverstone)\b.*\b(rm|hx|focus|hydro|toughpower|straight power|prime|leadex|ne|gm|sf|atx)\s*[- ]?\d{3,4}', title, re.I):
        return 'psu'
    if re.search(r'메인보드|마더보드|motherboard|mainboard|\b[ABHXYZQ][0-9]{3}[A-Z0-9-]*\b', title, re.I):
        return 'motherboard'
    if re.search(r'하드디스크|하드 드라이브|\bhdd\b|hard.?disk|barracuda|ironwolf|red plus|red pro|wd\d{2,4}ez|wd gold|exos\s*\d|ultrastar', title, re.I):
        return 'hdd'
    if re.search(r'\bssd\b|nvme|solid.?state|솔리드.?스테이트|990 pro|980 pro|sn770|sn850x|mp44|p3 plus', title, re.I):
        return 'ssd'
    if re.search(r'\b(ddr[345]|sodimm|so-dimm|ram)\b|램|메모리 모듈|메모리 킷|unbuffered dimm|registered dimm', title, re.I):
        return 'ram'
    if re.search(r'cpu\s*쿨러|쿨러/튜닝|수랭|수냉|공랭|라디에이터|일체형.?수.?냉|a\s*i\s*o\s*cooler|\bcpu cooler\b|liquid cooler|water cooler|system fan|case fan|cooling fan|fan cooler|liquid freezer|kraken\s*[0-9]|masterliquid|galahad|zoomflow|frostflow|peerless assassin|phantom spirit|dark rock|pure rock|hyper\s*\d|nh-u\d|ak\d{2,3}|ls\d{3}|assassin iv|thermalright.*(frozen|notte|phantom|peerless|assassin|spirit|frost)|darkflash.*twister|id[- ]?cooling.*(dashflow|frostflow|zoomflow|se-?\d{3}|is-?\d{2})|corsair.*\bh[0-9]{3,4}i\b|deepcool.*\b(ak|ls|lt|le|ag)\d{2,3}\b|asus.*rog (ryujin|ryuo)|cooler master.*(masterliquid|hyper|ml\d{3})', title, re.I):
        return 'cooler'
    # USB-C alone is not enough: hubs, docks and SSDs also use that connector.
    # Require explicit cable/adapter wording in product titles; the official cable
    # category stream can still admit model-only cable names through source taxonomy.
    if re.search(r'\b(케이블|cable|젠더|연장선|adapter|어댑터|ethernet cable|랜선)\b|(?:hdmi|displayport|\bdp\b|sata|usb[- ]?c|usb\s*[a-z0-9-]*)(?:\s*(?:to|↔|->|[-/])\s*[a-z0-9-]+)?\s*(?:케이블|cable)', title, re.I):
        return 'cable'
    if re.search(r'pc.?케이스|컴퓨터.?케이스|미들타워|빅타워|미니타워|\bpc case\b|computer case|\bchassis\b|\bmid.?tower\b|\bmini.?tower\b', title, re.I):
        return 'case'
    if re.search(r'geforce|\brtx\s*\d|\bgtx\s*\d|radeon|라데온|그래픽카드|\bgpu\b|\bvga\b', title, re.I):
        return 'gpu'
    if re.search(r'라이젠|ryzen|코어 울트라|core\s*ultra|core\s*i[3579]|코어\s*i[3579]|xeon|제온|threadripper|스레드리퍼|\bcpu\b|processor|\bi[3579]-?\d{4,5}[a-z]*\b', title, re.I):
        return 'cpu'
    # Wattage-heavy familiar PSU model patterns when the product name lacks "PSU".
    if re.search(r'\b\d{3,4}\s*w\b', title, re.I) and re.search(r'\b(atx|sfx|80\s*plus|80plus|hydro|toughpower|focus|rm\d|hx\d|prime|leadex|mwe|mag a\d|be quiet)\b', title, re.I):
        return 'psu'
    return ''

def classify_category(name, category_text='', fallback=''):
    by_path = _category_from_text(category_text)
    if by_path:
        return by_path
    by_title = _category_from_title(name)
    if by_title:
        return by_title
    return fallback or 'other'

def _as_image_url(value):
    if isinstance(value, str):
        return valid_url(value)
    if isinstance(value, dict):
        for k in ('url','src','image_url','imageUrl','img_url','image_link','imageLink','thumbnail','large','original','main'):
            u = valid_url(value.get(k))
            if u:
                return u
    if isinstance(value, list):
        for item in value:
            u = _as_image_url(item)
            if u:
                return u
    return ''

def _find_image_deep(value, depth=0):
    if depth > 3:
        return ''
    if isinstance(value, dict):
        for k, v in value.items():
            if re.search(r'image|img|photo|thumbnail|thumb', str(k), re.I):
                u = _as_image_url(v)
                if u:
                    return u
        for v in value.values():
            if isinstance(v, (dict, list)):
                u = _find_image_deep(v, depth + 1)
                if u:
                    return u
    elif isinstance(value, list):
        for v in value:
            u = _find_image_deep(v, depth + 1)
            if u:
                return u
    return ''

def _expand_product_variants(rows):
    """Yield parent catalogue rows and real, identified variant SKU rows.

    Danawa sometimes returns one product family with a `variants` list. When a
    variant has its own product_id, it is a real separate catalog SKU; keep its
    own price/name where supplied, inherit only shared presentation metadata, and
    never manufacture a new ID.
    """
    for parent in rows:
        if not isinstance(parent, dict):
            continue
        yield parent
        variants = parent.get('variants')
        if not isinstance(variants, list):
            continue
        parent_name = str(parent.get('name') or parent.get('product_name') or parent.get('title') or parent.get('productName') or '').strip()
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            variant_id = str(variant.get('product_id') or variant.get('productId') or variant.get('pcode') or variant.get('id') or variant.get('externalId') or '').strip()
            if not variant_id:
                continue
            merged = dict(parent)
            merged.pop('variants', None)
            merged.update(variant)
            variant_name = str(variant.get('name') or variant.get('product_name') or variant.get('title') or variant.get('productName') or '').strip()
            label = str(variant.get('variant_name') or variant.get('variantName') or variant.get('option_name') or variant.get('optionName') or variant.get('label') or variant.get('option_text') or variant.get('optionText') or '').strip()
            if not variant_name:
                merged['name'] = parent_name + (f' ({label})' if label and label.casefold() not in parent_name.casefold() else '')
            # A parent's price must never be copied onto a variant with no own price.
            price_keys = ('lowest_price','lowestPrice','price','price_krw')
            if not any(k in variant and variant.get(k) not in (None, '') for k in price_keys):
                for key in price_keys:
                    merged.pop(key, None)
            yield merged

def extract_products(data, fallback_category=''):
    """Normalize only product rows that have a real product image URL."""
    rows = result_rows(data)
    candidates = []
    if not rows:
        rows = list(recursive_dicts(data))
    for x in _expand_product_variants(rows):
        if not isinstance(x, dict):
            continue
        row_type = str(x.get('row_type') or x.get('rowType') or 'product').casefold()
        if row_type not in ('product', ''):
            continue
        name = str(x.get('name') or x.get('product_name') or x.get('title') or x.get('productName') or '').strip()
        pid = str(x.get('product_id') or x.get('productId') or x.get('pcode') or x.get('id') or x.get('externalId') or '').strip()
        if not name or not pid:
            continue
        img = ''
        for k in ('image_url','imageUrl','image','thumbnail','img','images','thumbnail_url','thumbnailUrl','product_image','productImage','product_img','img_url','imgUrl','image_link','imageLink','main_image','mainImage','photo','photos'):
            img = _as_image_url(x.get(k))
            if img:
                break
        if not img:
            img = _find_image_deep(x)
        if not img:
            continue
        url = valid_url(x.get('url') or x.get('product_url') or x.get('productUrl'))
        if not url and pid.isdigit():
            url = 'https://prod.danawa.com/info/?pcode=' + pid
        price = price_num(x.get('lowest_price') or x.get('price') or x.get('lowestPrice') or x.get('price_krw'))
        brand = str(x.get('maker') or x.get('brand') or x.get('manufacturer') or '').strip()
        # Read category evidence as separate hints instead of joining unrelated fields
        # into one artificial breadcrumb (which can make a CPU item inherit a cooler path).
        category_hints = []
        category_id_hints = []
        ordered_keys = (
            'category_path','categoryPath','breadcrumb','breadcrumbs','category_name_path','categoryNamePath',
            'leaf_category','leafCategory','category_3','category3Name','category_name','categoryName',
            'category_text','categoryText','categories','category_2','category2Name','category_1','category1Name',
            'parent_category','parentCategory','category'
        )
        for k in ordered_keys:
            v = x.get(k)
            if isinstance(v, (str, int)):
                sv = str(v).strip()
                if sv.isdigit():
                    mapped = DANAWA_CATEGORY_ID_MAP.get(sv)
                    if mapped: category_id_hints.append(mapped)
                elif sv:
                    category_hints.append(sv)
            elif isinstance(v, (dict, list)):
                for obj in recursive_dicts(v):
                    if isinstance(obj, dict):
                        for nk in ('path','categoryPath','category_path','leaf_name','leafName','name','title','label','category_name','categoryName'):
                            nv = obj.get(nk)
                            if isinstance(nv, str) and nv.strip(): category_hints.append(nv.strip())
                        for ik in ('id','category_id','categoryId','code'):
                            iv = str(obj.get(ik) or '').strip()
                            mapped = DANAWA_CATEGORY_ID_MAP.get(iv)
                            if mapped: category_id_hints.append(mapped)
        for k in ('category_id','categoryId','cate_id','cateId','category_code','categoryCode'):
            cid = str(x.get(k) or '').strip()
            mapped = DANAWA_CATEGORY_ID_MAP.get(cid)
            if mapped: category_id_hints.append(mapped)
        category_text = ''
        for hint in category_hints:
            if _category_from_text(hint):
                category_text = hint
                break
        if not category_text and category_id_hints:
            category_text = category_id_hints[0]
        detected = classify_category(name, category_text, fallback_category)
        candidates.append({
            'id': 'danawa:' + pid, 'externalId': pid, 'category': detected,
            'brand': brand, 'name': name, 'priceKrw': price or None, 'currency': 'KRW',
            'imageUrl': img, 'url': url, 'source': '다나와 가격비교 API',
            'updatedAt': now_iso(), 'priceStatus': 'live' if price else 'not_available',
            '_categoryText': category_text,
        })
    seen, out = set(), []
    for item in candidates:
        if item['id'] not in seen:
            seen.add(item['id'])
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

def _query_targets_category(category, query):
    q = str(query or '').casefold()
    patterns = {
        'cpu': r'cpu|processor|ryzen|라이젠|인텔 코어|프로세서|xeon|threadripper',
        'motherboard': r'메인보드|motherboard|mainboard|\bb[4568]\d{2}\b|\bz\d{3}\b|\bx\d{3}\b|am5',
        'ram': r'메모리|\bddr[345]\b|\bram\b|sodimm|so-dimm|memory|ecc',
        'gpu': r'그래픽카드|geforce|radeon|\brtx\b|\bgtx\b|graphics card',
        'ssd': r'\bssd\b|nvme|sata ssd|solid.?state',
        'hdd': r'\bhdd\b|하드디스크|hard.?disk|nas hdd',
        'cooler': r'쿨러|수랭|수냉|공랭|cooler|cooling|fan|라디에이터|튜닝',
        'case': r'케이스|pc case|computer case|chassis|타워',
        'psu': r'파워|psu|power supply|\d{3,4}\s*w|80plus|80 plus',
        'software': r'windows|윈도우|office|microsoft\s*365|소프트웨어|운영체제|한컴|백신|license|라이선스',
        'cable': r'케이블|cable|hdmi|displayport|\bdp\b|usb|sata 데이터|랜선|ethernet',
    }
    return bool(re.search(patterns.get(category, r'(?!)'), q, re.I))

def _category_relevance(item, requested_category, stream_has_category_id=False, stream_query=''):
    """Prefer explicit product-title identity, then exact taxonomy, then focused streams."""
    name = str(item.get('name') or '')
    cat_text = str(item.get('_categoryText') or '')
    path_cat = _category_from_text(cat_text)
    title_cat = _category_from_title(name)
    low_name = name.casefold()
    if title_cat == 'unrelated':
        return False
    # A confidently identified product title wins over a vague/bad breadcrumb.
    # This prevents CPUs from inheriting cooler/board labels from wrapper metadata.
    if title_cat and title_cat not in ('other',):
        return title_cat == requested_category
    # Reject explicit taxonomy conflicts when the title itself is not decisive.
    if path_cat and path_cat not in (requested_category, 'other'):
        return False
    if path_cat == requested_category:
        return True
    # Avoid putting hubs and docking stations in the cable tab unless the item is actually a cable.
    if requested_category == 'cable' and re.search(r'허브|\bhub\b|도킹|\bdock\b|멀티포트|분배기|카드리더', low_name, re.I) and not re.search(r'케이블|\bcable\b|랜선', low_name, re.I):
        return False
    # Only accept an unlabelled item when the source category ID or focused search itself
    # indicates the intended type. This prevents cooler/board/air-conditioner leakage.
    focused = _query_targets_category(requested_category, stream_query)
    if requested_category in {'software','cable'}:
        # These categories have short product names and inconsistent category breadcrumbs.
        # Permit focused software/cable queries after explicit title conflicts and hub/dock
        # exclusions above have been applied; otherwise the tabs can go completely empty.
        return bool(stream_has_category_id or focused)
    if requested_category in {'cpu','gpu','ram','ssd','hdd','motherboard'}:
        return bool(stream_has_category_id or focused)
    if requested_category in {'psu','case','cooler'}:
        return bool(stream_has_category_id or focused)
    return False

def _search_total(data):
    if not isinstance(data, dict):
        return 0
    for key in ('total_results','totalResults','total','catalog_total','total_count','totalCount','total_products'):
        value = data.get(key)
        try:
            if value is not None and int(value) >= 0:
                return int(value)
        except (ValueError, TypeError):
            pass
    return extract_total(data, 0)

def _new_stream(query, category_id=''):
    return {
        'query': query, 'category_id': category_id or '', 'page': 0,
        'has_more': True, 'total': 0, 'signatures': set(),
        'last_added': 0, 'last_raw': 0, 'last_normalized': 0,
        'last_used': time.time(), 'failed': False,
    }

def _fetch_stream_page(stream, page):
    payload = {'query': stream['query'], 'scope': 'catalog', 'page': page}
    # Category IDs are used only where an established parent/category has been
    # observed; query-only streams broaden subcategories without mixing fake data.
    if stream.get('category_id'):
        payload['category'] = stream['category_id']
    return reef_post('/danawa/v1/search', payload)

def _schedule_catalog_prefetch(query, category, page, page_size):
    key = (str(query or '').casefold(), str(category or '').casefold(), int(page), int(page_size))
    with PREFETCH_LOCK:
        if key in PREFETCH_IN_FLIGHT:
            return
        PREFETCH_IN_FLIGHT.add(key)
    try:
        future = PREFETCH_EXECUTOR.submit(live_search, query, category, page, page_size, False)
    except RuntimeError:
        with PREFETCH_LOCK:
            PREFETCH_IN_FLIGHT.discard(key)
        return
    def clear(_future):
        with PREFETCH_LOCK:
            PREFETCH_IN_FLIGHT.discard(key)
    future.add_done_callback(clear)


def live_search(q, category, page, page_size, _schedule_prefetch=True):
    request_started = time.monotonic()
    category = CATEGORY_MAP.get(str(category or '').strip().lower(), str(category or '').strip().lower())
    category = category if category in set(CATEGORY_DEFAULT_QUERY) else ''
    original_query = (q or '').strip()[:160]
    page = max(1, min(50, int(page)))
    page_size = max(1, min(40, int(page_size)))

    is_category_browse = (not original_query) or (bool(category) and original_query.casefold() in CATEGORY_GENERIC_TERMS)
    query = CATEGORY_DEFAULT_QUERY.get(category, 'PC 부품') if is_category_browse else original_query
    wanted = page * page_size
    category_id = DANAWA_CATEGORY_IDS.get(category, '') if is_category_browse else ''
    cache_key = (query.casefold(), category, category_id or '')

    # Serve already-warmed page slices without waiting for the lock held by a prefetch.
    cached_snapshot = LIVE_SEARCH_CACHE.get(cache_key)
    if cached_snapshot and time.time() - cached_snapshot.get('updated_at', 0) <= LIVE_SEARCH_CACHE_TTL and len(cached_snapshot.get('items', [])) >= wanted:
        start_idx = (page - 1) * page_size
        selected = [dict(x) for x in cached_snapshot['items'][start_idx:start_idx + page_size]]
        snapshot_more = len(cached_snapshot['items']) > start_idx + page_size or any(
            st.get('has_more') and st.get('page', 0) < 50 for st in cached_snapshot['streams'].values()
        ) or bool(cached_snapshot['next_plan_index'] < len(cached_snapshot['plan']))
        result = {
            'items': selected, 'total': max(cached_snapshot.get('primary_total', 0), len(cached_snapshot['items'])),
            'page': page, 'pageSize': page_size, 'currency': 'KRW', 'updatedAt': now_iso(),
            'source': 'Danawa via ReefAPI', 'live': True, 'serverBuild': SERVER_BUILD, 'hasMore': snapshot_more,
            'diagnostics': {
                'requestedCategory': category or None, 'queryUsed': query,
                'streamsLoaded': len(cached_snapshot['streams']), 'callsThisRequest': 0,
                'cachedUniqueProducts': len(cached_snapshot['items']), 'returnedProducts': len(selected),
                'primaryQueryReportedTotal': cached_snapshot.get('primary_total', 0),
                'totalIsExactUniqueCount': len(cached_snapshot['streams']) <= 1,
                'cacheHit': True, 'cacheTtlSeconds': LIVE_SEARCH_CACHE_TTL,
                'elapsedMs': round((time.monotonic() - request_started) * 1000),
            },
            'note': '캐시된 실제 공급사 상품을 반환했습니다. 가격은 별도 가격 API 연결 상태에 따라 달라질 수 있습니다.'
        }
        if _schedule_prefetch and is_category_browse and selected and snapshot_more and page < 49:
            _schedule_catalog_prefetch(original_query, category, page + 1, page_size)
        return result

    with LIVE_SEARCH_LOCK:
        lock = LIVE_SEARCH_KEY_LOCKS.setdefault(cache_key, threading.RLock())

    with lock:
        now = time.time()
        entry = LIVE_SEARCH_CACHE.get(cache_key)
        if not entry or now - entry.get('updated_at', 0) > LIVE_SEARCH_CACHE_TTL:
            plan = []
            if query:
                # Prefer a real leaf-category stream when it is the same phrase as
                # the default query (e.g. USB cable). Keep query-only fallbacks only
                # when their phrases differ so we do not pay twice for the same rows.
                extra_streams = DANAWA_EXTRA_CATEGORY_STREAMS.get(category, []) if is_category_browse and category else []
                same_phrase_stream = next((spec for spec in extra_streams if spec[0].casefold() == query.casefold()), None)
                if same_phrase_stream:
                    plan.append(same_phrase_stream)
                else:
                    plan.append((query, category_id or ''))
                if is_category_browse and category:
                    plan.extend(spec for spec in extra_streams if spec != same_phrase_stream)
                    plan.extend((alt, '') for alt in CATEGORY_FALLBACK_QUERIES.get(category, []))
            # Preserve ordering and remove duplicate stream specs.
            seen_specs, deduped_plan = set(), []
            for spec in plan:
                if spec not in seen_specs:
                    seen_specs.add(spec); deduped_plan.append(spec)
            entry = {
                'query': query, 'category': category, 'items': [], 'ids': set(),
                'streams': {}, 'plan': deduped_plan, 'next_plan_index': 0,
                'updated_at': now, 'primary_total': 0, 'max_total': 0,
                'errors': [], 'raw_rows': 0, 'normalized': 0,
                'calls_this_request': 0, 'cache_hits': 0,
            }
            LIVE_SEARCH_CACHE[cache_key] = entry

        calls = 0
        max_calls = 10
        max_workers = 10
        def stream_id(spec):
            return spec[0].casefold() + '||' + (spec[1] or '')

        def ensure_stream(spec):
            sid = stream_id(spec)
            if sid not in entry['streams']:
                entry['streams'][sid] = _new_stream(spec[0], spec[1])
            return sid, entry['streams'][sid]

        def merge_stream_page(sid, stream, source_page, data):
            raw_rows = result_rows(data)
            normalized = extract_products(data, fallback_category=category)
            source_total = _search_total(data)
            if source_page == 1 and stream is entry['streams'].get(stream_id(entry['plan'][0])):
                entry['primary_total'] = max(entry['primary_total'], source_total)
            entry['max_total'] = max(entry['max_total'], source_total)
            stream['total'] = max(stream['total'], source_total)
            stream['last_raw'] = len(raw_rows)
            stream['last_normalized'] = len(normalized)
            stream['page'] = source_page
            stream['last_used'] = time.time()
            signature = tuple(str(x.get('externalId') or x.get('id') or '') for x in normalized)
            if signature and signature in stream['signatures']:
                stream['has_more'] = False
                stream['last_added'] = 0
                print(f"[catalog-search] repeated source page: category={category} query={stream['query']!r} page={source_page}", flush=True)
                return
            if signature:
                stream['signatures'].add(signature)
            new_items = []
            added = 0
            for item in normalized:
                if category and not _category_relevance(item, category, bool(stream.get('category_id')), stream.get('query', '')):
                    continue
                item['category'] = category or item.get('category') or 'other'
                item.pop('_categoryText', None)
                pid = str(item.get('id') or '')
                if not pid or pid in entry['ids']:
                    continue
                entry['ids'].add(pid)
                entry['items'].append(item)
                new_items.append(item)
                added += 1
            stream['last_added'] = added
            # ReefAPI's Danawa search has 50 pages; a repeated/empty upstream page
            # ends this stream. Do not trust a premature has_more=false when the
            # returned total proves that more rows should exist.
            has_more_value = data.get('has_more') if isinstance(data, dict) else None
            stream['has_more'] = bool(raw_rows) and source_page < 50
            if has_more_value is False and source_total and source_total <= source_page * 40:
                stream['has_more'] = False
            if not raw_rows:
                stream['has_more'] = False
            entry['raw_rows'] = len(raw_rows)
            entry['normalized'] = len(normalized)
            entry['updated_at'] = time.time()
            if new_items:
                # Persist every real, image-backed product fetched, not just the
                # 24 visible cards. The DB grows organically as searches run.
                cache_products(new_items)
            print(
                f"[catalog-search] build={SERVER_BUILD} cat={category or 'all'} query={stream['query']!r} "
                f"source_page={source_page} rows={len(raw_rows)} normalized={len(normalized)} "
                f"accepted={added} cumulative={len(entry['items'])} total={source_total}", flush=True)

        def next_missing_stream_specs():
            pending = []
            for spec in entry['plan']:
                sid = stream_id(spec)
                if sid not in entry['streams']:
                    pending.append(spec)
            return pending

        # Fill enough unique visible products. Start with one query. If it returns
        # too few image-backed records, broaden to category-specific query streams
        # in parallel instead of making six sequential requests for one phrase.
        while len(entry['items']) < wanted and calls < max_calls:
            batch = []
            pending_specs = next_missing_stream_specs()
            if pending_specs:
                batch_specs = pending_specs[:min(max_workers, max_calls - calls)]
                for spec in batch_specs:
                    sid, stream = ensure_stream(spec)
                    batch.append((sid, stream, 1))
            else:
                available = []
                for sid, stream in entry['streams'].items():
                    if stream.get('has_more') and stream.get('page', 0) < 50 and not stream.get('failed'):
                        available.append((sid, stream, stream['page'] + 1))
                if not available:
                    break
                # Request the next page of the stream that has produced the most
                # products recently; tie-break on source page to balance streams.
                available.sort(key=lambda row: (row[1].get('last_added', 0), -row[2]), reverse=True)
                batch = available[:min(max_workers, max_calls - calls)]
            if not batch:
                break
            outcomes = []
            with ThreadPoolExecutor(max_workers=min(max_workers, len(batch))) as pool:
                futures = [(sid, stream, source_page, pool.submit(_fetch_stream_page, stream, source_page))
                           for sid, stream, source_page in batch]
                for sid, stream, source_page, fut in futures:
                    try:
                        outcomes.append((sid, stream, source_page, fut.result(), None))
                    except Exception as exc:
                        outcomes.append((sid, stream, source_page, None, str(exc)))
            calls += len(batch)
            # Merge in deterministic plan order so pagination is stable.
            plan_order = {stream_id(spec): i for i, spec in enumerate(entry['plan'])}
            outcomes.sort(key=lambda out: (plan_order.get(out[0], 9999), out[2]))
            for sid, stream, source_page, data, error in outcomes:
                if error:
                    stream['failed'] = True
                    stream['has_more'] = False
                    entry['errors'].append({'query': stream['query'], 'page': source_page, 'error': error[:180]})
                    print(f"[catalog-search] source error query={stream['query']!r} page={source_page}: {error}", flush=True)
                    continue
                merge_stream_page(sid, stream, source_page, data)
            if not pending_specs and all(not st.get('has_more') for st in entry['streams'].values()):
                break

        start_idx = (page - 1) * page_size
        selected = entry['items'][start_idx:start_idx + page_size]
        # Do not sum totals across overlapping queries; that would advertise fake
        # unique counts. The count is the supplier's primary-query total, while
        # diagnostics expose the number of distinct products actually cached.
        total = max(entry['primary_total'], len(entry['items']))
        total_exact = len(entry['streams']) <= 1
        has_more = len(entry['items']) > start_idx + page_size or any(
            st.get('has_more') and st.get('page', 0) < 50 for st in entry['streams'].values()
        ) or bool(next_missing_stream_specs())
        diagnostics = {
            'requestedCategory': category or None,
            'queryUsed': query,
            'streamsLoaded': len(entry['streams']),
            'callsThisRequest': calls,
            'cachedUniqueProducts': len(entry['items']),
            'returnedProducts': len(selected),
            'primaryQueryReportedTotal': entry['primary_total'],
            'totalIsExactUniqueCount': total_exact,
            'recentRawRows': entry.get('raw_rows', 0),
            'recentNormalizedProducts': entry.get('normalized', 0),
            'variantSkuExpansion': True,
            'errors': entry['errors'][-4:],
            'cacheTtlSeconds': LIVE_SEARCH_CACHE_TTL,
            'parallelSearchLimit': max_calls,
            'streamPlanCount': len(entry['plan']),
            'elapsedMs': round((time.monotonic() - request_started) * 1000),
        }
        result = {
            'items': selected, 'total': total, 'page': page, 'pageSize': page_size,
            'currency': 'KRW', 'updatedAt': now_iso(), 'source': 'Danawa via ReefAPI',
            'live': True, 'serverBuild': SERVER_BUILD, 'hasMore': has_more,
            'diagnostics': diagnostics,
            'note': '실제 이미지가 있는 공급사 상품만 표시합니다. 여러 검색어와 카테고리 트리를 병합해 중복을 제거합니다. 전체 고유 상품 수는 대량 수집 전까지 검증되지 않습니다.'
        }

        # Prefetch one UI page asynchronously. After the first page appears, this
        # starts warming the next page in the same cache so later clicks are quick.
        # Only browse tabs are prefetched; arbitrary user queries stay on-demand.
        if _schedule_prefetch and is_category_browse and selected and has_more and page < 49:
            _schedule_catalog_prefetch(original_query, category, page + 1, page_size)
        return result

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
