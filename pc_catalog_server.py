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

def extract_products(data, fallback_category=""):
 # Provider fields may evolve; only normalize records that have a recognizable name and a real product URL/image.
 candidates=[]
 for x in recursive_dicts(data):
  name=str(x.get('name') or x.get('product_name') or x.get('title') or x.get('productName') or '').strip()
  pid=str(x.get('product_id') or x.get('productId') or x.get('pcode') or x.get('id') or '').strip()
  if not name or not pid: continue
  img=valid_url(x.get('image_url') or x.get('imageUrl') or x.get('image') or x.get('thumbnail') or x.get('img'))
  url=valid_url(x.get('url') or x.get('product_url') or x.get('productUrl'))
  if not url and pid.isdigit(): url='https://prod.danawa.com/info/?pcode='+pid
  if not img: continue
