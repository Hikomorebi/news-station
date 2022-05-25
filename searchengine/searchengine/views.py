from django.shortcuts import render, HttpResponse

import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch()

def query2date(str,date_format="%Y-%m-%d %H:%M:%S"):
    date = datetime.datetime.strptime(str, date_format)
    return date


def matchPhraseContent(key_context):#短语查询
    res = es.search(index='nku_news',body={"query": {"match_phrase": {"newsContent": key_context}}, "size": 1000})
    tt = res['hits']['hits']
    tt.sort(key=lambda a:a['_score']+10*a['_source']['pagerank'],reverse = True)
    num = res['hits']['total']['value']
    list = []
    for i in range(min(num,1000)):

        list.append(tt[i]['_source'])

    return list
def anchor(key_context):#锚文本查询
    res = es.search(index='nku_news',body={"query": {"match": {"anchor_text": key_context}}, "size": 1000})
    tt = res['hits']['hits']
    tt.sort(key=lambda a:a['_score']+10*a['_source']['pagerank'],reverse = True)
    num = res['hits']['total']['value']
    list = []
    for i in range(min(num,1000)):

        list.append(tt[i]['_source'])

    return list
def default(key_context):#默认查询方式
    res = es.search(index='nku_news',body={"query": {"match": {"newsContent": key_context}}, "size": 1000})
    tt = res['hits']['hits']
    tt.sort(key=lambda a:a['_score']+10*a['_source']['pagerank'],reverse = True)
    num = res['hits']['total']['value']
    list = []
    for i in range(min(num,1000)):

        list.append(tt[i]['_source'])

    return list


def wildcardContent(key_context):#内容通配查询
    res = es.search(index='nku_news',body={"query": {"wildcard": {"newsContent": key_context}}, "size": 1000})
    tt = res['hits']['hits']
    tt.sort(key=lambda a:a['_score']+10*a['_source']['pagerank'],reverse = True)
    num = res['hits']['total']['value']
    list = []
    for i in range(min(num,1000)):

        list.append(tt[i]['_source'])

    return list

def searchUrl(key_context):#url查询
    res = es.search(index='nku_news',body={"query": {"match_phrase": {"newsUrl": key_context}}, "size": 1000})
    tt = res['hits']['hits']
    tt.sort(key=lambda a:a['_score']+10*a['_source']['pagerank'],reverse = True)
    num = res['hits']['total']['value']
    list = []
    for i in range(min(num,1000)):

        list.append(tt[i]['_source'])

    return list
def termTitle(key_context):#标题精准查询
    res = es.search(index='nku_news',body={"query": {"term": {"title": key_context}}, "size": 1000})
    tt = res['hits']['hits']
    tt.sort(key=lambda a:a['_score']+10*a['_source']['pagerank'],reverse = True)
    num = res['hits']['total']['value']
    list = []
    for i in range(min(num,1000)):

        list.append(tt[i]['_source'])

    return list
def searchByDate(begin_str,end_str):#通过时间查询
    begin = query2date(begin_str)
    end = query2date(end_str)
    if begin > end :
        print("开始时间不得小于结束时间")
        exit()
    condition = {
        'query' : {
            'range' : {
                'newsPublishTime' : {
                    'gte' : begin,
                    'lte' : end
                }
            }
        },
        'size' : 1000
    }
    res = es.search(index='nku_news',body=condition)
    tt = res['hits']['hits']
    tt.sort(key=lambda a: a['_score'] * a['_source']['pagerank'], reverse=True)
    num = res['hits']['total']['value']
    list = []
    for i in range(min(num, 1000)):

        list.append(tt[i]['_source'])

    return list

def merge_list(list1,list2):
    res = []
    for i in list1:
        if i in list2:
            res.append(i)

    return res

#y = merge_list(searchByDate('2019-10-01 12:00:00','2019-11-01 12:00:00'),searchByDate('2019-10-01 12:00:00','2019-12-01 12:00:00'))


def handle(qt, qc):
    if qt=='phrase':
        return matchPhraseContent(qc)
    elif qt =='wilecard':
        return wildcardContent(qc)
    elif qt=='term':
        return termTitle(qc)
    elif qt=='time':
        queryt = qc.split('/')
        print(queryt)
        return searchByDate(queryt[0].strip(),queryt[1].strip())
    elif qt=='anchor':
        return anchor(qc)
    elif qt=='url':
        return searchUrl(qc)
    else:
        return default(qc)


class Query:
    def __init__(self):
        es = Elasticsearch()

    def standard_search(self, query):

        li = query.split('|')

        ans_list = []
        for s in li:
            p1 = s.find(':')
            qt = s[:p1].strip()
            qc = s[p1 + 1:].strip()
            ans_list.append(handle(qt, qc))

        if len(ans_list) == 1:
            return ans_list[0]
        else:
            temp = ans_list[0]
            for i in range(1, len(ans_list)):
                temp = merge_list(temp, ans_list[i])
            return temp



    def __exit__(self, exc_type, exc_val, exc_tb):
        print('Query close.')




def search_form(request):
    return render(request, 'main.html')

def search(request):
    res = None
    q = Query()
    if 'q' in request.GET and request.GET['q']:
        res = q.standard_search(request.GET['q'])
        c = {
            'query': request.GET['q'],
            'resAmount': len(res),
            'results': res,
        }
    else:
        return render(request, 'main.html')

    # str = ''
    # for i in res:
    #     str += '<p><a href="' + i['newsUrl'] + '">' + i['newsTitle'] + '</a></p>'
    # return HttpResponse(str)

    return render(request, 'result.html', c)



