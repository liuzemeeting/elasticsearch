import pymysql
import json
from datetime import datetime
from elasticsearch import Elasticsearch

# 连接数据库
# conn = pymysql.connect(
#     host='192.168.6.200',
#     port=5306,
#     user='tbkt_shuying',
#     passwd='shuXueEng@kt2017!',
#     db='ziyuan_new',
#     charset='utf8',
# )


class ElasticSearchClass(object):
 
    def __init__(self, host, port, user, passwrod):
        self.host = host
        self.port = port
        self.user = user
        self.password = passwrod
        self.connect()
 
    def connect(self):
        self.es = Elasticsearch(hosts=[{'host': self.host, 'port': self.port}],
                                http_auth=(self.user, self.password ))
                    
    def insertDocument(self, index, type, body, id=None):
        '''
        插入一条数据body到指定的index、指定的type下;可指定Id,若不指定,ES会自动生成
        :param index: 待插入的index值
        :param type: 待插入的type值
        :param body: 待插入的数据 -> dict型
        :param id: 自定义Id值
        :return:
        '''
        return self.es.index(index=index, doc_type=type, body=body, id=id)
 
    def count(self, indexname):
        """
        :param indexname:
        :return: 统计index总数
        """
        return self.conn.count(index=indexname)
 
    def delete(self, indexname, doc_type, id):
        """
        :param indexname:
        :param doc_type:
        :param id:
        :return: 删除index中具体的一条
        """
        self.es.delete(index=indexname, doc_type=doc_type, id=id)
 
    def get(self, doc_type, indexname, id):
        return self.es.get(index=indexname,doc_type=doc_type, id=id)

    def searchindex(self, index):
        """
        查找所有index数据
        """
        try:
            return self.es.search(index=index)
        except Exception as err:
            print(err)

    def searchDoc(self, index=None, type=None, body=None):
        '''
        查找index下所有符合条件的数据
        :param index:
        :param type:
        :param body: 筛选语句,符合DSL语法格式
        :return:
        '''
        return self.es.search(index=index, doc_type=type, body=body)

    def search(self,index,type,body,size=10,scroll='10s'):
        """
        根据index，type查找数据，
        其中size默认为十条数据，可以修改为其他数字，但是不能大于10000
        """
        return self.es.search(index=index, doc_type=type,body=body,size=size,scroll=scroll)

    # def search(self,index,type,body,size=10000):
    #     """
    #     根据index，type查找数据，
    #     其中size默认为十条数据，可以修改为其他数字，但是不能大于10000
    #     """
    #     return self.es.search(index=index, doc_type=type,body=body,size=size)

    def scroll(self, scroll_id, scroll):
        """
        根据上一个查询方法，查询出来剩下所有相关数据
        """
        return self.es.scroll(scroll_id=scroll_id, scroll=scroll)


obj = ElasticSearchClass("59.110.41.175", "9200", "", "")  # 连接elasticsearch客户端

# 查询所有数据
# body = {
#     "query":{
#         "match_all":{}
#     }
# }
# body = {
#     "query" : {
#         "match" : {
#             "data.content" : "数问12C6021答案A知识点分数乘分数的计算方法第2问121809602A20B08C答案A如12L"
#         }
#     }
# }
# body = {
#     "query" : {
#         "match" : {
#             "data.content" : "一根铁丝"
#         }
#     }
# }
# #数据的查询
# mm = obj.searchDoc(index="question",type="text",body=body)
# print(mm)


#多个条件查询
# body = {
#   "query": {
#     "bool": {
#       "should": [
#         { "match": { "data.content":  "一根铁丝" }},
#         { "match": { "data.question_content": "李阿姨"   }},
#         { "match": { "data.ask_content.content": '李阿姨' }}
#       ],
#     "filter": {
#             # "range" : {
#             #     "data.ask_content.ask_id" : { "gt" : 60000 }  
#             # }
#         }


#     }
#   }
# }
# body = {
#     "query" : {
#         "match" : {
#             "data.content" : "一根铁丝",
#             "minimum_should_match": "75%"
#         }
#     }
# }
body = {
  "query": {
    "bool": {
      "should": [
        { "match": { "data.content":  "一根铁丝" }},
        { "match": { "data.question_content": "一根铁丝"  }},
        { "match": { "data.ask_content.content": '一根铁丝' }}
      ],
    }
  }
}
# body = {
#   "query": {
#     "bool": {
#       "must": [
#         {"match": {
#             "data.content": "李阿姨"
#         }},
#         {"match": {
#             "data.question_content": "李阿姨"
#         }},
#         {"match": {
#             "data.ask_content.content": "李阿姨"
#         }}        
#       ]
#     }
#   }
# }
# body = {
#     "query" : {
#         "match_phrase" : {
#             "data.question_content": "李阿姨"
#         },
#         "highlight": {
#             "fields" : {
#                     "data.question_content": "李阿姨"
#                 }
#             }
#     },
# }

# # 数据的查询
response = obj.search(index="question", type="text", size=10, body=body, scroll='100s')
# response = obj.search(index="question",type="text",body=body,size=10000)
print(response)
xiabiaos = []  # 存储所有数据id
data = response["hits"]["hits"]
m = 0
for i in data:
    # m += 1
    # print(i)
    # if str(id) == str(i["_id"]):
    #     print(m)
    #     break
    xiabiaos.append(i["_id"])
data_total = response["hits"]["total"]
print(xiabiaos)
print(data_total)
datalength = int(int(data_total)/10)+1
print(type(datalength))
# 拿到数据后 循环scroll_id 直到取出所有数据
for i in range(0,datalength):
    zz = response["_scroll_id"]
    body = {
        "scroll": "10s", 
        "_scroll_id" : zz
    }
    response = obj.scroll(scroll_id=zz, scroll="100s",)  
    data = response["hits"]["hits"]
    for i in data:
        xiabiaos.append(i["_id"])

print(xiabiaos)
print(len(xiabiaos))