import datetime
import json
import os

from elasticsearch import Elasticsearch, client
from elasticsearch import helpers

def str2date(str_date,date_format="%Y-%m-%d %H:%M:%S"):
    str_date+=':00'
    date = datetime.datetime.strptime(str_date,date_format)
    return date

def createIndex():
    ic = client.IndicesClient(es)
    if ic.exists(index="nku_news"):
        es.indices.delete(index="nku_news")
        print("删除之前存在的index")
    if not ic.exists(index="nku_news"):
        settings = {
            "mappings": {
                "properties": {
                    "title": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_max_word"
                    },
                    "newsurl": {
                        "type": "keyword"
                    },
                    "newsFrom": {
                        "type": "keyword"
                    },
                    "newsPublishTime": {
                        "type": "date"
                    },
                    "newsContent": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_max_word"
                    },
                    "anchor_text" : {
                        "type" : "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_max_word"
                    },

                    "pagerank": {
                        "type": "double"
                    }
                }
            }
        }


        ic.create(index="nku_news",ignore=400,body=settings)
        print("创建index成功！")






def insertData():
    with open('./news.json', 'r', encoding='utf-8') as f:
        for line in f.readlines():
            doc = line.strip('\n').strip(',')
            docj = json.loads(doc)
            stime = docj['newsPublishTime']
            del docj['newsPublishTime']
            docj['newsPublishTime'] = str2date(stime)
            print(docj)
            es.index(index='nku_news',body = docj)
    '''
    body = []
    if begin+5000  <= file_nums:
        filepaths_a = filepaths[begin:begin+5000]
    else:
        filepaths_a = filepaths[begin:]
    for doc in filepaths_a:
        dict_doc = {}
        dict_doc_bool = {}
        with open(doc,'rb') as file_obj:
            while True:
                line = file_obj.readline().decode('utf-8','ignore')
                if line.strip() == '':
                    break

                colon = line.find(':')
                if colon != -1:
                    pre_key = line[0:colon]
                    if pre_key not in dict_doc:
                        dict_doc[pre_key] = line[colon + 1:].strip()
                        dict_doc_bool[pre_key] = True
                    else:
                        dict_doc_bool[pre_key] = False
                else:
                    if dict_doc_bool[pre_key]:
                        dict_doc[pre_key] += line.strip()

            context = file_obj.read().decode('utf-8','ignore').strip()
            data1 = {
                "Message-ID": dict_doc["Message-ID"] if "Message-ID" in dict_doc else "",
                "Date": str2date(dict_doc["Date"]) if "Date" in dict_doc else "",
                "From": dict_doc["From"] if "From" in dict_doc else "",
                "To": dict_doc["To"] if "To" in dict_doc else "",
                "Subject": dict_doc["Subject"] if "Subject" in dict_doc else "",
                "Mime-Version": dict_doc["Mime-Version"] if "Mime-Version" in dict_doc else "",
                "Content-Type": dict_doc["Content-Type"] if "Content-Type" in dict_doc else "",
                "Content-Transfer-Encoding": dict_doc[
                    "Content-Transfer-Encoding"] if "Content-Transfer-Encoding" in dict_doc else "",
                "X-From": dict_doc["X-From"] if "X-From" in dict_doc else "",
                "X-To": dict_doc["X-To"] if "X-To" in dict_doc else "",
                "X-cc": dict_doc["X-cc"] if "X-cc" in dict_doc else "",
                "X-bcc": dict_doc["X-bcc"] if "X-bcc" in dict_doc else "",
                "X-Folder": dict_doc["X-Folder"] if "X-Folder" in dict_doc else "",
                "X-Origin": dict_doc["X-Origin"] if "X-Origin" in dict_doc else "",
                "X-FileName": dict_doc["X-FileName"] if "X-FileName" in dict_doc else "",
                "path": doc,
                "context": context
            }
            every_body = \
                {
                    "_index": 'nku_news',
                    "_source": data1
                }

            body.append(every_body)
    helpers.bulk(es, body , raise_on_error=False)
    print("插入%d条数据" %len(filepaths_a))
    '''

if __name__ == "__main__":
    es = Elasticsearch()

    createIndex()
    insertData()


