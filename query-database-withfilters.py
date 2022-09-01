import argparse
from datetime import datetime, timedelta
from os import system

from bson.objectid import ObjectId
from pymongo import MongoClient

from getonbrd import *


def query_mongo(batch_n=100, days_lookback=30):
    client = MongoClient('mongodb://localhost:27017/')
    my_db = client['getonbrd_job_offers']
    collections = my_db.list_collection_names()
    
    max_lookback = datetime.now()-timedelta(days=days_lookback)
    query_jobs = []
    for collection in collections:
        coll = my_db[collection]
        cursor = coll.aggregate([
            {'$project':{
                '_id':0,
                'published_at':1,
                'seniority':1,
                'public_url':1,
                'title':1,
                'company':1,
                'description':1,
                'projects':1,
                'functions':1,
                'benefits':1,
                'desirable':1,
            }
            },
            {'$sort':{
                'published_at':-1
            }
            },
            
            {'$match':{
                'published_at':{
                    '$gte':max_lookback,
                }
            }
            },
            {'$limit':batch_n},
        ])
        for job in cursor:
            query_jobs.append(job)
        continue
    return query_jobs

def filter_text_body(data:list, seniority=False):
    data_filtered = []
    key_words_ok = [
        'python',
        'streamlit','postman'
        'etl','git',
    ]
    key_words_bad = [
        'año','años', 'de', 'experiencia', '1', '2',
        'java', 'javascript', 'frontend', 'sql',
        'flask', 'django',
    ]
    attributes = ['description','projects','functions','benefits','desirable',]
    for job in data:
        longstring = ''
        for attribute in attributes:
            longstring+=job.get(attribute) #type:str()
        # PARSE string into array.
        longstring = longstring.strip()
        longstring = longstring.replace(",", "")
        longstring = longstring.replace(":", " ")
        longstring = longstring.replace("-", " ")
        longstring = longstring.replace("<br>", " ") #This must be done in method:parse
        longstring = longstring.lower()
        words_array = longstring.split()
        unique_words_array = list(set(words_array))
        
        found_kws = [kw for kw in key_words_ok if kw in unique_words_array]
        phrase = [word for word in key_words_bad if word in unique_words_array]
        
        # FILTER
        if seniority:
            if not (job.get('seniority') == ('junior' or 'sin_experiencia')):
                continue
        
        flag_kws = True if found_kws else False
        if (flag_kws):
            job.update(dict(kw_ok= found_kws.copy()))
            job.update(dict(kw_red= phrase.copy()))
            data_filtered.append(job)
    data_filtered.sort(key=lambda d: d['published_at'], reverse=True)
    return data_filtered

def parse_inputs(pargs=None):
    parser = argparse.ArgumentParser(description='Query database through CLI. Filtered by keywords:( python|etl|git )')
    
    parser.add_argument('--update',
        action='store_true',
        help='Update datatabse before query.'
    )
    parser.add_argument('--seniority',
        action='store_true',
        help='Filter results by seniority.'
    )
    parser.add_argument('--output',
        action='store',
        type=int,
        default=10,
        help='How many results for display. n=10'
    )
    parser.add_argument('--batch_query',
        action='store',
        type=int,
        default=100,
        help='n=100, default.'
    )
    parser.add_argument('--lookback_date',
        action='store',
        type=int,
        default=30,
        help='Thirty days, by default.'
    )
    return parser.parse_args(pargs)

def run_script(args=None):
    arg = parse_inputs(args)
    
    # UPDATE DATABASE BEFORE.
    if arg.update:
        categories = [
            'machine-learning-ai',
            'data-science-analytics',
            'mobile-developer',
            'sysadmin-devops-qa',
            'programming',
        ]
        update_jobs_collection(categories)
    
    system('clear')
    
    query = query_mongo(batch_n=arg.batch_query, days_lookback=arg.lookback_date)
    jobs = filter_text_body(data=query, seniority=arg.seniority)
    
    for idx, job in enumerate(jobs[:arg.output]):
        print(f"\n\n({idx})({job.get('seniority')}) {job.get('published_at')} // {job.get('title')} // {job.get('company')} // {job.get('public_url')}")
        print(f"MATCH: {job.get('kw_ok')}\tWEAK: {job.get('kw_red')}")
    
    print(f"\n_Total results: {len(jobs)}\n")


if __name__== '__main__':
    run_script()