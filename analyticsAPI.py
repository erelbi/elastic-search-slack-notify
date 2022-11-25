from connect_els import ElasticABC
from elasticsearch_dsl import Search,Q
import ssl,os



 

class Analytics_API_Dev(ElasticABC):
    def __init__(self):
        self.index_nginx_error = "nginx-raw*"
        self.elastic_auth_dev = os.environ['Analytics_Auth']
        self.connect = None
        self.now = None
        self.past = None
        self.size_int = 50000
        self.request_timeout = 30000
        self.datetime()
        
    def datetime(self):
        try:
            datetime_result = super().datetime()
            self.now = datetime_result[0]
            self.past = datetime_result[1]
            
        except Exception as err:
            """add logging module"""
            print(err)
    

    def check_connection(self):
        try:
            self.connect = super().check_connection(self.elastic_auth_dev)
            if  self.connect is not None:
                return self.connect
        except:
            raise Exception("Sorry, cannot connect")

   
    def analytic_search(self,url,now=None,past=None):
        if now is None or past is None:
            now = self.now
            past = self.past
        try:
            query_str = {"bool": {"must": [{"range": {"@timestamp": {"gte": past, "lte": now}}}]}}
            raw_data = Search(using=self.connect, index=self.index_nginx_error ).params(request_timeout=self.request_timeout).query(query_str).filter("term", server_name="{}".format(url))
            return raw_data  
        except:
            raise Exception("Failed analytic_search")


    @staticmethod
    def analytic_search_hit_miss(obj):
        try:
            obj.aggs.bucket("hit_miss", "terms", field="proxy_cache_status")
            response = obj.execute()
            return response.to_dict()['aggregations']['hit_miss']['buckets']
        except Exception as err:
            raise Exception("Sorry, analytic_search_hit_miss!")
    
    @staticmethod
    def analytic_search_request(obj):
        try:
            obj.aggs.bucket("request_time", "terms", field="request_time")
            response = obj.execute()
            return response.to_dict()['aggregations']['request_time']['buckets']
        except Exception as err:
            raise Exception("Sorry, analytic_search_request!")
    
