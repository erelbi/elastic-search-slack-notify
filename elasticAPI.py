from connect_els import ElasticABC
from elasticsearch_dsl import Search,Q
import ssl,os



class Elastic_API_Dev(ElasticABC):
    size_int = 50000 # Bu size değerini büyük tuttuk çünkü  farklı zaman aralıklarında  büyük veri çekilebilir
    def __init__(self):
        self.index_nginx_error = "nginxerror-*"
        self.connect = None
        self.now = None
        self.past = None
        self.datetime()
        self.elastic_auth_dev = os.environ['Kibana_Auth']
        self.request_timeout = 3000
        

    def check_connection(self):
        try:
            self.connect = super().check_connection(self.elastic_auth_dev)
            if  self.connect is not None:
                return self.connect
        except:
            raise Exception("Sorry, cannot connect")
    
    
    def datetime(self):
        try:
            datetime_result = super().datetime()
            self.now = datetime_result[0]
            self.past = datetime_result[1]
        except Exception as err:
            """add logging module"""
            print(err)
    

    def elastic_search(self,now=None,past=None):
        if now is None or past is None:
            now = self.now
            past = self.past
        try:
            query_str = {"bool": {"must": [{"range": {"@timestamp": {"gte": past, "lte": now}}}]}}
            raw_data = Search(using=self.connect, index="{}".format(self.index_nginx_error)).params(request_timeout=self.request_timeout).query(query_str)
            time_group_data = raw_data.query(query_str)
            return time_group_data
        except Exception as err:
            raise Exception("Sorry, datetime_different_search failed!")
    

    @staticmethod
    def server_error_count(obj):
        try:
            obj.aggs.bucket("server_name", "terms", field="errorhost.keyword",size=Elastic_API_Dev.size_int) # size path change !   
            response = obj.execute()
            return response.to_dict()['aggregations']['server_name']['buckets']
        except Exception as err:
            raise Exception("Sorry, server_error_count failed")
    
    @staticmethod
    def server_msg_keyword(obj):
        """ gün sonunda tek bir metod dan parametre ile çekilsin gerekiyorsa ABC """
        try:
            obj.aggs.metric("server_name", "terms", field="errorhost.keyword",size=Elastic_API_Dev.size_int)
            obj.aggs['server_name'].bucket('msg_keyword','terms', field="errormessage.keyword")
            response = obj.execute()
            return response.to_dict()['aggregations']['server_name']['buckets']
        except Exception as err:
            raise Exception("Sorry, server_msg_keyword")
    

    
    @staticmethod
    def server_request_keyword(obj):
        try:
            obj.aggs.metric("server_name", "terms", field="errorhost.keyword",size=Elastic_API_Dev.size_int)
            obj.aggs['server_name'].bucket('request_keyword','terms', field="request.keyword")
            response = obj.execute()
            #print(response.to_dict()['aggregations']['server_name']['buckets'])
            return response.to_dict()['aggregations']['server_name']['buckets']
        except Exception as err:
            raise Exception("Sorry, server_msg_keyword")


            


    


        




   
