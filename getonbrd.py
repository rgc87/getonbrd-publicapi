import argparse

from datetime import datetime
import pickle
from time import sleep

import requests
from bson.objectid import ObjectId
from pymongo import MongoClient

# INDEX INFO
''' 
cursor_mdb = coll.list_indexes()
cursor_mdb = coll.index_information() #BETTER 
'''

# Public api.
baseurl = 'https://www.getonbrd.com/api/v0/'

# Database client.
mongo_client = MongoClient('mongodb://localhost:27017/')
my_db = mongo_client['getonbrd_job_offers']


def _insert_documents_avoiding_duplicates(data, category):
    try:
        parsed_data = _parse_jobs(data)
    except Exception as e:
        print(e)
        print(f'From {__name__}. Array of jobs comming from _update_jobs_collection() should be wrong.')
    category = category.replace('-','_')
    coll_name = f'jobs_{category}'
    cursor_mongo = my_db[coll_name]
    
    duplicated = 0
    for j in parsed_data:
        try:
            res = cursor_mongo.insert_one(j)
            if res:
                print('Updating jobs...',j.get('published_at'), j.get('company'))
        except:
            duplicated+=1
            print(f'ERROR by Duplication with ',j.get('published_at'), j.get('company'))
    return f'Total duplicated files:{duplicated}'

def update_jobs_collection(category:str):
    # First request_get.
    per_page = 10
    page = 1
    endpoint = f'categories/{category}/jobs?per_page={per_page}&page={page}&expand=["company"]'
    url = baseurl+endpoint
    
    jobs = _request(url)
    total_pages = jobs['meta'].get('total_pages')
    all_jobs = []
    print(f'Ready, page {page} of {total_pages}')
    
    newest_stored_data = _read_newest_db(category)
    new_data = datetime.fromtimestamp(jobs['data'][0]['attributes'].get('published_at'))
    if not (new_data > newest_stored_data):
        print(f'Database is up to date at: {newest_stored_data}')
        return 
    
    if total_pages > 1:
        all_jobs+=jobs['data']
        for _ in range(total_pages-1):
            page+=1
            url = f'{baseurl}categories/{category}/jobs?per_page={per_page}&page={page}&expand=["company"]'
            jobs = _request(url)
            all_jobs+=jobs['data']
            print(f'Ready, page {page} of {total_pages}')
            
            new_data = datetime.fromtimestamp(all_jobs[0]['attributes'].get('published_at'))
            if not (new_data > newest_stored_data):
                print('There is no more new publications.')
                return _insert_documents_avoiding_duplicates(data=all_jobs, category=category)
            sleep(3)
        # Just in case of read every page.
        
        return _insert_documents_avoiding_duplicates(data=all_jobs, category=category)
    else:
        all_jobs += jobs['data']
        return _insert_documents_avoiding_duplicates(data=all_jobs, category=category)


def _read_newest_db(category:str):
    category = category.replace('-','_')
    colls = my_db[f'jobs_{category}']
    cursor_mdb = colls.aggregate([
        {'$sort':{'published_at':-1}},
        {'$limit':1},
        {'$project':{'title':1,'company':1,'published_at':1}},
    ])
    jobs_sample = []
    for doc in cursor_mdb:
        jobs_sample.append(doc)
    return jobs_sample[0]['published_at'] #datetime

def _request(url, headers={}, payload={}):
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code==200:
            return response.json()
    except:
        return response.status_code

def _parse_jobs(jobs:list):
    jobs_raw = jobs.copy()
    
    with open('data/dict_indxs_tags.pckl', 'rb') as bindata:
        idx_tags = pickle.load(bindata)
    
    with open('data/dict_seniority_types.pckl', 'rb') as bindata:
        seniority_types = pickle.load(bindata)
    
    with open('data/dict_modality_types.pckl', 'rb') as bindata:
        modality_types = pickle.load(bindata)
    
    # Unpack and reduce data.
    for data in jobs_raw:
        # DELETE KEY:TYPE
        data.pop('type')
            
        # UNPACK DICT:LINKS.
        data.update( data['links'] )
        data.pop('links')
        
        # UNPACK DICT:ATTRIBUTES.
        for k,v in data['attributes'].items():
            data.update({k:v})
        
        # DELETE DICT:ATTRIBUTES
        data.pop('attributes')
    
    chars_html = [  
        '<p>','</p>','<li>','</li>','<strong>','</strong>',
        '<ul>','</ul>','<div>','</div>',
    ]
    attributes = [
        'description','projects','functions',
        'benefits','desirable',
    ]
    for j in jobs_raw:
        # Delete html characters.
        for attr in attributes:
            for char in chars_html:
                j[attr] = j[attr].replace(char, '')
        
        # Parse dates.
        j['published_at'] = datetime.fromtimestamp(j['published_at'])
        
        # Parse company.
        j['company'] = j['company']['data']['attributes'].get('name')
        
        # Parse tags id.
        tags_data = j['tags']['data'].copy()
        j['tags'] = [ idx_tags.get(x.get('id')) for x in tags_data ]
        
        # Parse seniority id.
        j['seniority'] = seniority_types.get( j['seniority']['data'].get('id') )
        
        # Parse modality id.
        j['modality'] = modality_types.get( j['modality']['data'].get('id') )
    return jobs_raw

def _read_every_category_page(category:str):
    # First request_get.
    per_page = 10
    page = 1
    endpoint = f'categories/{category}/jobs?per_page={per_page}&page={page}&expand=["company"]'
    url = baseurl+endpoint
    
    jobs = _request(url)
    total_pages = jobs['meta']['total_pages']
    all_jobs = []
    
    if total_pages > 1:
        all_jobs += jobs['data']
        print(f'{category} Page {page} of {total_pages}')
        for _ in range(total_pages-1):
            page+=1
            url = f'{baseurl}categories/{category}/jobs?per_page={per_page}&page={page}&expand=["company"]'
            jobs = _request(url)
            all_jobs += jobs['data']
            print(f'{category} Page {page} of {total_pages}')
            sleep(3)
        return all_jobs
    else:
        print(f'{category} Page {page} of {total_pages}')
        all_jobs += jobs['data']
        return  all_jobs

def database_getonbrd_fromscratch(categories):
    for cat in categories:
        # RETRIEVE DATA.
        get_all_jobs = _read_every_category_page(category=cat)
        jobs_parsed = _parse_jobs(get_all_jobs) #***1
        
        #PERSIST WITH PICKLE.
        timestamp = datetime.now()
        filename = f"data/list_{cat}_{timestamp}.pckl"
        with open(filename, 'wb') as bindata:
            pickle.dump(jobs_parsed, bindata)
        
        # PERSIST ON MONGO DB.
        c = cat.replace('-','_')
        collection=f'jobs_{c}'
        colls = my_db[collection]
        try:
            result = colls.insert_many(jobs_parsed)
            colls.create_index(
                "published_at",
                unique=True,
            )
            print(f"Persisted, created: ",cat)
        except Exception as e:
            print("Algo falló en: ",cat)
    return 'FINISHED, from scratch.'

def database_from_pickle(filename:str):
    with open(filename) as bindata:
        parsed_jobs:list = pickle.load()
        return parsed_jobs


def parse_inputs(pargs=None):
    parser = argparse.ArgumentParser(description='...')
    
    parser.add_argument('--create',
        action='store_true',
        help='Create database from scratch.'
    )
    parser.add_argument('--update',
        action='store_true',
        help='Request for new data and compare.'
    )
    parser.add_argument('--quehace',
        action='store_true',
        help='Holis.'
    )
    return parser.parse_args(pargs)

def run_script(args=None):
    arg = parse_inputs(args)
    
    categories = [
        'machine-learning-ai',
        'data-science-analytics',
        'mobile-developer',
        'sysadmin-devops-qa',
        
        'programming', 
    ]
    if arg.ejemplo:
        print('Hello world')
        return
    elif arg.create:
        create = database_getonbrd_fromscratch(categories)
        print(create)    
    elif arg.update:
        update = update_jobs_collection(category='mobile-developer')
        print(update)


if __name__== '__main__':
    run_script()