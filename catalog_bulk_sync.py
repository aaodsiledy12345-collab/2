#!/usr/bin/env python3
"""Incrementally index real image-backed product records returned by the configured ReefAPI.
This does not fabricate products and cannot exceed the provider's accessible catalogue/quota.
Run where REEF_API_KEY is set, e.g. as a one-off Render shell job.
"""
import argparse, os, sys, time
from pathlib import Path
import pc_catalog_server as api

QUERIES = {
 'cpu': ['AMD Ryzen CPU','Intel Core CPU','Intel Core Ultra','AMD Threadripper','Intel Xeon'],
 'motherboard': ['AM5 메인보드','AM4 메인보드','LGA1851 메인보드','LGA1700 메인보드','Mini ITX 메인보드'],
 'ram': ['DDR5 메모리','DDR4 메모리','노트북 RAM','서버 메모리 ECC','메모리 32GB'],
 'gpu': ['NVIDIA RTX 그래픽카드','GeForce GTX 그래픽카드','AMD Radeon 그래픽카드','워크스테이션 그래픽카드'],
 'ssd': ['NVMe SSD','SATA SSD','M.2 SSD','외장 SSD'],
 'hdd': ['3.5인치 HDD','2.5인치 HDD','NAS 하드디스크'],
 'psu': ['ATX 파워서플라이','SFX 파워서플라이','850W 파워','1000W 파워'],
 'case': ['미들타워 PC 케이스','미니타워 PC 케이스','빅타워 PC 케이스','Mini ITX 케이스'],
 'cooler': ['CPU 공랭 쿨러','CPU 수랭 쿨러','120mm 케이스 팬','140mm 케이스 팬'],
 'other': ['PC 케이블','PC 확장 카드','PC 주변기기']
}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--pages',type=int,default=3,help='pages per query (1-50)')
    ap.add_argument('--page-size',type=int,default=40,help='items requested per page (1-40)')
    ap.add_argument('--pause',type=float,default=0.4,help='seconds between provider calls')
    ap.add_argument('--category',choices=sorted(QUERIES),default=None)
    args=ap.parse_args()
    if not api.REEF_API_KEY:
        print('ERROR: REEF_API_KEY is missing; no data was fetched.',file=sys.stderr); return 2
    api.init_db()
    pages=max(1,min(50,args.pages)); size=max(1,min(40,args.page_size))
    query_groups={args.category:QUERIES[args.category]} if args.category else QUERIES
    calls=0; received=0
    for category, queries in query_groups.items():
        for query in queries:
            for page in range(1,pages+1):
                try:
                    data=api.reef_post('/danawa/v1/search',{'query':query,'scope':'catalog','page':page,'page_size':size,'pageSize':size,'limit':size})
                    items=api.extract_products(data)
                    # Trust only actual image-backed records from the provider. Don't invent prices or photos.
                    for item in items:
                        if item.get('category')=='other' and category!='other':
                            item['category']=category
                    items=[x for x in items if x.get('imageUrl') and x.get('url')]
                    api.cache_products(items)
                    calls+=1; received+=len(items)
                    total=api.extract_total(data,len(items))
                    print(f'[{calls}] {category} | {query} | page {page}: {len(items)} products (provider total={total}, local DB={api.count_products():,})',flush=True)
                    # If provider explicitly indicates no next pages, don't waste API quota.
                    if not items or (total and page*size>=total): break
                except Exception as exc:
                    print(f'WARN {category} | {query} | page {page}: {exc}',file=sys.stderr,flush=True)
                    break
                time.sleep(max(0,args.pause))
    print(f'DONE: calls={calls}, received_records={received}, unique_local_products={api.count_products():,}')
    print('This is incremental indexing, not a guarantee of 1,000,000 products. Continue only within provider permissions and quota.')
    return 0
if __name__=='__main__': raise SystemExit(main())
