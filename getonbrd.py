import requests
from datetime import datetime
from time import sleep
import pickle
from bson.objectid import ObjectId
from pymongo import MongoClient


# Public api.
baseurl = 'https://www.getonbrd.com/api/v0/'

# Database client.
client = MongoClient('mongodb://localhost:27017/')


def insert_documents(data:list, collection:str, db_name:str):
    """
        Insert data into mongoDB
        """
    my_db = client[db_name]
    colls = my_db[collection]
    result = colls.insert_many(jobs)
    return result.inserted_ids

def query_documents(collection:str, db_name:str, pipeline:list):
    """
        QUERY data from mongoDB
        """
    my_db = client[db_name]
    colls = my_db[collection]
    
    pipeline = []
    cursor_mdb = colls.aggregate(pipeline)
    
    trabajos = []
    for doc in cursor_mdb:
        trabajos.append(doc)
    return trabajos

def _request(url, headers={}, payload={}):
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code==200:
            return response.json()
    except:
        return response.status_code

def read_every_category_page(category:str, per_page:int):
    # First request_get.
    page = 1
    expand = '["company"]'
    endpoint = f'categories/{category}/jobs?per_page={per_page}&page={page}&expand={expand}'
    url = baseurl+endpoint
    
    jobs = _request(url)
    
    total_pages = jobs['meta']['total_pages']
    all_jobs = []
    
    for _ in range(total_pages):
        all_jobs+=jobs['data']
        print(f'Page {page} : READY... of {total_pages}')
        
        # Update.
        page+=1
        sleep(2)
        url = f'{baseurl}categories/{category}/jobs?per_page={per_page}&page={page}&expand={expand}'
        # Request again
        jobs = _request(url)
    return all_jobs

def _unpack_and_reduce(rawdata):
    data_unpacked = rawdata.copy()
    for idx,data in enumerate(data_unpacked):
        # Delete key:type
        data_unpacked[idx].pop('type')
        # Unpack dict:links.
        data_unpacked[idx].update( data_unpacked[idx]['links'] )
        data_unpacked[idx].pop('links')
        # Unpack dict:attributes.
        for k,v in data_unpacked[idx]['attributes'].items():
            data_unpacked[idx].update({k:v})
        # Delete dicy:attributes
        data_unpacked[idx].pop('attributes')
    return data_unpacked

def parse_jobs(jobs):
    jobs_raw = _unpack_and_reduce(jobs).copy()
    
    with open('data/dict_indxs_tags.pckl', 'rb') as bindata:
        idx_tags = pickle.load(bindata)
    
    with open('data/dict_seniority_types.pckl', 'rb') as bindata:
        seniority_types = pickle.load(bindata)
    
    with open('data/dict_modality_types.pckl', 'rb') as bindata:
        modality_types = pickle.load(bindata)
    
    chars_html = ['<p>','</p>','<li>','</li>','<strong>','</strong>','<ul>','</ul>',]
    attributes = ['description','projects','functions','benefits','desirable',]
    
    for idx,j in enumerate(jobs_raw):
        # Delete html characters.
        for char in chars_html:
            for attr in attributes:
                jobs_raw[idx][attr] = j[attr].replace(char, '')
        
        # Parse dates.
        jobs_raw[idx]['published_at'] = datetime.fromtimestamp(j['published_at'])
        
        # Parse company.
        jobs_raw[idx]['company'] = j['company']['data'].get('id')
        
        # Parse tags id.
        tags_data = j['tags']['data'].copy()
        jobs_raw[idx]['tags'] = [ idx_tags.get(x.get('id')) for x in tags_data ]
        
        # Parse seniority id.
        jobs_raw[idx]['seniority'] = seniority_types.get( j['seniority']['data'].get('id') )
        
        # Parse modality id.
        jobs_raw[idx]['modality'] = modality_types.get( j['modality']['data'].get('id') )
        
        
        #########################################################################
        #########################################################################
    return jobs_raw

def _iterator(iterable):
    for i in iterable:
        yield i


if __name__== '__main__':
    # tags_gob = dict(zip([n for n in range(len(all_tags))], all_tags ))
    # with open('dict_indxs_tags.pckl', 'wb') as binario:
        # pickle.dump(tags_gob, binario)
    
    from getonbrd import *
    
    # ALL JOBS, PROGRAMMING.
    get_all_jobs = read_every_category_page(category='programming', per_page=10)
    
    # Persist.
    with open('data/jobs_programming_440.pckl', 'wb') as bindata:
        pickle.dump(get_all_jobs, bindata)
        
    # Open bin.
    with open('data/jobs_programming_440.pckl', 'rb') as bindata:
        programming_jobs = pickle.load(bindata)
    
    programming_jobs[0].keys()
    programming_jobs
    
    
    insert_documents(data=get_jobs, collection='jobs_programming', db_name='getonbrd_api')
    
    itefunc = _iterator(resp)
    next(itefunc)
    
    # Filter by Tags.
    for j in jobs_parsed:
        # if 'data-engineer' in j['attributes']['tags']:
        # if 'data-science' in j['attributes']['tags']:
        if 'mongo' in j['attributes']['tags']:
            print(f" {j['attributes']['published_at']} {j['attributes']['title']} {j['attributes']['tags']}")