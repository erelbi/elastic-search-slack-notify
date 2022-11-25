from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from elasticAPI import Elastic_API_Dev
from analyticsAPI import Analytics_API_Dev


app = FastAPI()


class Connection:
    def __init__(self):
        self.API_kbn = Elastic_API_Dev()
        self.API_anl = Analytics_API_Dev()
        self.API_kbn.check_connection()
        self.API_anl.check_connection()
    
    def getter_kbn(self):
        return self.API_kbn
    
    def getter_anl(self):
        return self.API_anl

  


class API_Class:
    
    @staticmethod
    def customer_error_count(customer_cls,con):
        try:
            API= con.getter_kbn()
            data = API.elastic_search(now=customer_cls.datetime_now,past=customer_cls.datetime_past)
            error_customer=Elastic_API_Dev.server_error_count(obj=data) 
            for host in error_customer:
                if host['key'] == customer_cls.customer:
                    return host['doc_count']
        except Exception as err:
            raise Exception(" Failed customer_error_count ")
    
    @staticmethod
    def customer_error_msg_keyword(customer_cls,con):
        try:
            API= con.getter_kbn()
            data = API.elastic_search(now=customer_cls.datetime_now,past=customer_cls.datetime_past)
            msg_keyword = Elastic_API_Dev.server_msg_keyword(data)
            for host in msg_keyword:
                if host['key'] == customer_cls.customer:
                    return host['msg_keyword']['buckets']        
        except:
            raise Exception("Failed customer_error_msg_keyword ")

    @staticmethod
    def customer_error_request_url(customer_cls,con):
        API= con.getter_kbn()
        data = API.elastic_search(now=customer_cls.datetime_now,past=customer_cls.datetime_past)
        request_keyword = Elastic_API_Dev.server_request_keyword(data) 
        for host in request_keyword:
                if host['key'] == customer_cls.customer:
                    return host['request_keyword']['buckets']
    
    @staticmethod
    def customer_hit_miss_anl(customer_cls,con):
        try:
            API= con.getter_anl()
            data = API.analytic_search(now=customer_cls.datetime_now,past=customer_cls.datetime_past,url=customer_cls.customer)  
            return Analytics_API_Dev.analytic_search_hit_miss(data)
        except:
            raise Exception("customer_hit_miss_anl Faied!")
    
    @staticmethod
    def customer_request_time_anl(customer_cls,con):
        try:
            API = con.getter_anl()
            data = API.analytic_search(now=customer_cls.datetime_now,past=customer_cls.datetime_past,url=customer_cls.customer)
            return Analytics_API_Dev.analytic_search_request(data)
        except:
            raise Exception("customer_request_time_anl")
        



class CustomerSchema(BaseModel):
    customer: str
    error_count: bool
    message: bool
    request: bool
    hit_miss: bool
    request_time: bool
    count: int
    datetime_now: Optional[str] = None
    datetime_past: Optional[str] = None 



    @classmethod
    def add_customer(cls,customer_cls,con):
        try:        
            if customer_cls.count > 0 and customer_cls.customer is not None:
                _attribute_list = CustomerSchema.only_true_attr(customer_cls)
                response = CustomerSchema.manager_attribute(_attribute_list,customer_cls,con)
                if response is not None:
                    return {'status':200, 'data':response}
                else:
                    return {'status':401, 'data':response}
            else:
                return{'status':400,'data':None}
        except Exception as err:
            print(err)
    
    @staticmethod
    def only_true_attr(customer_cls) -> list:
        attr_list=list()
        for key,value in customer_cls:
            if  value==True:
                attr_list.append(key)
        return attr_list
    
    @staticmethod
    def count_control(customer_cls,response):
        try:
            if response['error_count'] > customer_cls.count:
                return response
            else:
                return None
        except:
            raise Exception("count control failed!")

    @staticmethod
    def manager_attribute(attribute: list,customer_cls,con):
        try:
            attribute_dict = {

                    "error_count" : API_Class.customer_error_count,
                    "message" : API_Class.customer_error_msg_keyword,
                    "request" : API_Class.customer_error_request_url,
                    "hit_miss" : API_Class.customer_hit_miss_anl,
                    "request_time" : API_Class.customer_request_time_anl


            }
            response =dict()
            for attr in attribute_dict:
                if attr in attribute:
                    response[attr] = attribute_dict[attr](customer_cls,con)
            
            return CustomerSchema.count_control(customer_cls,response)
        except Exception as err:
            print(err)
            return None


        



@app.post('/')
async def search_customer(customer: CustomerSchema):
    return CustomerSchema.add_customer(customer,con=Connection())