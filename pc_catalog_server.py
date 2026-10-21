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
  price=price_num(x.get('lowest_price') or x.get('price') or x.get('lowestPrice') or x.get('price_krw'))
  brand=str(x.get('maker') or x.get('brand') or x.get('manufacturer') or '').strip()
  category='ssd' if re.search(r'\bssd\b|NVMe',name,re.I) else 'hdd' if re.search(r'\bHDD\b|하드디스크',name,re.I) else 'ram' if re.search(r'\bDDR[3454]\b|메모리|램',name,re.I) else 'cpu' if re.search(r'라이젠|코어 울트라|Core i[3579]|Xeon|프로세서',name,re.I) else 'gpu' if re.search(r'RTX|GTX|Radeon|라데온|그래픽카드|GeForce',name,re.I) else 'motherboard' if re.search(r'메인보드|마더보드|B[456789]\d{2}|X[3678]\d{2}|Z[6789]\d{2}',name,re.I) else 'psu' if re.search(r'파워서플라이|전원공급장치|\b\d{3,4}W\b',name,re.I) else 'case' if re.search(r'PC 케이스|컴퓨터 케이스|미들타워|빅타워',name,re.I) else 'cooler' if re.search(r'쿨러|수랭|수냉|라디에이터',name,re.I) else 'other'
  candidates.append({'id':'danawa:'+pid,'externalId':pid,'category':category,'brand':brand,'name':name,'priceKrw':price or None,'currency':'KRW','imageUrl':img,'url':url,'source':'다나와 가격비교 API','updatedAt':now_iso(),'priceStatus':'live' if price else 'not_available'})
 # deduplicate IDs preserving order
 seen=set(); out=[]
 for x in candidates:
  if x['id'] not in seen: seen.add(x['id']); out.append(x)
 return out

def extract_total(data, fallback):
 # Preserve the provider's catalogue-wide count when it supplies one.
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
 # Upstream API owns pagination; do not slice a second time, which drops later-page products.
 data=reef_post('/danawa/v1/search',{'query':q or category or 'PC 부품','scope':'catalog','page':page,'page_size':page_size,'pageSize':page_size,'limit':page_size})
 items=extract_products(data)
 if category: items=[x for x in items if x['category']==category or x['category']=='other']
 total=extract_total(data, len(items))
 selected=items[:page_size]
 cache_products(selected)
 return {'items':selected,'total':total,'page':page,'pageSize':page_size,'currency':'KRW','updatedAt':now_iso(),'source':'Danawa via ReefAPI','live':True,
 'note':'현재 페이지의 상품만 표시합니다. 전체 상품 수와 페이지 범위는 공급사 API가 제공하는 검색/페이지네이션에 따릅니다. 실제 이미지 URL가 있는 상품만 표시합니다.'}

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
     if not q: q={'ram':'DDR5 RAM','ssd':'SSD','cpu':'CPU','gpu':'그래픽카드','motherboard':'메인보드'}.get(cat,'PC 부품')
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
 ap=argparse.ArgumentParser(); ap.add_argument('--import',dest='csv_path'); ap.add_argument('--import-only',action='store_true'); ap.add_argument('--db',default=str(DB_PATH)); ap.add_argument('--host',default=os.environ.get('HOST','0.0.0.0')); ap.add_argument('--port',type=int,default=int(os.environ.get('PORT','8787'))); a=ap.parse_args()
 DB_PATH=Path(a.db).resolve(); init_db()
 if a.csv_path: import_catalog_csv(Path(a.csv_path).resolve())
 if a.import_only: return
 print('PC 견적 API 서버 시작'); print(f'DB: {DB_PATH}'); print(f'상품 수(로컬 DB): {count_products():,}'); print(f'실시간 다나와 API: {"설정됨" if REEF_API_KEY else "미설정 - 임의 가격 미사용"}'); print(f'포트: {a.port}')
 ThreadingHTTPServer((a.host,a.port),Handler).serve_forever()
if __name__=='__main__': main()
