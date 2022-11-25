from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch


class ElasticABC(ABC):
    def __init__(self):
        self.es = None
    """Abstract Method"""
    """ check one! """
    @abstractmethod
    def check_connection(self,connect):
        try:
            self.es = Elasticsearch(connect)     
            if not self.es.ping():
                raise ValueError("Connection failed")
            return self.es
        except Exception as err:
            print(err)
            return None
    

    def datetime_convert(self,obj):
        try:
            year = '{:02d}'.format(obj.year)
            month = '{:02d}'.format(obj.month)
            day = '{:02d}'.format(obj.day)
            datetime_result = '{}-{}-{}'.format(year, month, day)
            return datetime_result
        except:
            raise Exception("Sorry,  datetime_convert failed!")

    @abstractmethod
    def datetime(self):
        try:
            now = datetime.now()
            past =  now - timedelta(days=1)
            return  "{}T00:00:00".format(self.datetime_convert(now)), "{}T00:00:00".format(self.datetime_convert(past))
        except:
            raise Exception("Sorry,  datetime failed!")