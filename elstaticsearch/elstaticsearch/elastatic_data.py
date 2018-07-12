# coding: utf-8
import time
import pymysql
from elasticsearch import Elasticsearch

conn = pymysql.connect(
    host='192.168.7.250',
    port=3308,
    user='wangshicheng',
    passwd='shicheng@tbkt2017!',
    db='tbkt_shuxue',
    charset='utf8',
)

class ElasticSearchClass(object):
    def __init__(self, host, port, user, passwrod):
        self.host = host
        self.port = port
        self.user = user
        self.password = passwrod
        self.connect()

    def connect(self):
        self.es = Elasticsearch(hosts=[{'host': self.host, 'port': self.port}],
                                http_auth=(self.user, self.password))

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
        return self.es.get(index=indexname, doc_type=doc_type, id=id)

    def searchindex(self, index):
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

    def search(self, index, type, body, size=1, scroll='10s'):
        try:
            return self.es.search(index=index, doc_type=type, body=body, size=size, scroll=scroll)
        except Exception as err:
            # print(err)
            if("404" in str(err)):
                return None
            else:
                dic = {'count': 0, '_shards': {'total': 0, 'successful': 0, 'failed': 0}}
                return "other"

    def scroll(self, scroll_id, scroll):
        """
        根据上一个查询方法，查询出来剩下所有相关数据
        """
        return self.es.scroll(scroll_id=scroll_id, scroll=scroll)

def search_data():
    sql = "select * from sx_searchdata where id = 8"
    # cursor = connection.cursor()
    obj = ElasticSearchClass("59.110.41.175", "9200", "", "")
    cursor =conn.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    for i in data:
        content = i[1]
        question_id = i[2]
        data_id = i[0]
    body = {
        "query": {
            "match": {
                "data.content": content,
            },
        }
    }
    response = obj.search(index="question", type="text", size=10, body=body, scroll='100s')
    data_total = response["hits"]["total"]
    datalength = int(int(data_total) / 10) + 1
    data = response["hits"]["hits"]
    id_list = []
    searchdata = []
    for m in data:
        finaldata = m["_source"]["data"]
        searchdata.append(finaldata)
    is_false = True
    for id in data:
        id_list.append(id["_id"])
        if str(question_id) == str(id["_id"]):
            bb = [i for i, x in enumerate(id_list) if x == str(question_id)]
            is_false = False
            sql = "UPDATE sx_searchdata set sequence = '%s' where id = '%s'" % (bb[0], data_id)
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            break
    if is_false:
        for i in range(0, datalength):
            zz = response["_scroll_id"]
            response = obj.scroll(scroll_id=zz, scroll="10s")
            data = response["hits"]["hits"]
            for m in data:
                id_list.append(m["_id"])
                if str(question_id) == str(m["_id"]):
                    print("9999999999999")
                    bb = [i for i, x in enumerate(id_list) if x == str(question_id)]
                    sql = "UPDATE sx_searchdata set sequence = '%s' where id = '%s'" % (bb[0], data_id)
                    cursor.execute(sql)
                    conn.commit()
                    cursor.close()
                    break
search_data()


