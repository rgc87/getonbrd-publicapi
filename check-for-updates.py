from datetime import datetime

from bson.objectid import ObjectId
from pymongo import MongoClient

from getonbrd import *


client = MongoClient('mongodb://localhost:27017/')
my_db = client['getonbrd_job_offers']
collections = my_db.list_collection_names()
collections.pop(3)

# UPDATE DATABASE.
categories = [
    'machine-learning-ai',
    'data-science-analytics',
    'mobile-developer',
    'sysadmin-devops-qa',
    
    'programming', 
]
update_jobs_collection(categories)

# Imprime en pantalla un query. Seniority
n = 30
for collection in collections:
    coll = my_db[collection]
    job_c = coll.aggregate([
        {'$project':{
            '_id':0,
            'published_at':1,
            'seniority':1,
            'public_url':1,
            'title':1,
            'company':1,
            }
        },
        
        {'$sort':{'published_at':-1}},
        {'$limit':n},
    ])
    for j in job_c:
        if j.get('seniority') == ('junior' or 'sin_experiencia'):
            print(f"\nSeniority: {j.get('seniority')}\t{j.get('published_at')}\t{j.get('public_url')}\n{j.get('title')} |¨_¨| Company: {j.get('company')}")
            


# Imprime en pantalla un query. Filter by Tags.