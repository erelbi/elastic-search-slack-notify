from elasticAPI  import Elastic_API_Dev
from analyticsAPI import Analytics_API_Dev
from data_vis import DataVisualization
from logger_mn  import Logger
import logging
import configparser
import requests
import json
import sys
import os




class Customer:
    def __init__(self,url):
        self.attr_list = list()
        self.customer_url = url
    
    def attr(self,attr):
        self.attr_list.append(attr)
    
        
class Customer_Response:
    def __init__(self,customer,error_count=None,msg=None,url=None,count_hm=None,request_time=None):
        self.customer = customer
        self.error_count = error_count
        self.message = msg
        self.request = url
        self.hit_miss = count_hm
        self.request_time = request_time
    

class NotifyConstructor():
    def __init__(self):
        self.customer = configparser.RawConfigParser()
        self.customer.read('customer.cfg')
        self.parse_customer()
        self.API_kbn = Elastic_API_Dev()
        self.API_anl = Analytics_API_Dev()
        self.API_kbn.check_connection()
        self.API_anl.check_connection()
        self.logger = Logger(d_e_b_u_g=False) # Hata alıyorsan cli den debug yapmak için True'ya çek ve çalıştır!


    
class NotifyBot(NotifyConstructor):

    def __init__(self):
        super().__init__()
        self.datetime_now = None
        self.datetime_past = None
        


    def parse_customer(self):
        try:
            self.customer_dict=dict()
            for url in self.customer.sections():
                self.customer_dict[url] = self.customer[url]
        except:
            raise Exception("parse_customer  Failed!")
        
         
    
    def customer_attribute(self,obj):
        try:
            for attr in obj:
                return attr
        except:
            raise Exception("customer_attribute Failed!")
        
    def customer_get_value(self):
        customer_list = list()  
        try:
            for url,obj in self.customer_dict.items():
                customer_object = Customer(url) 
                for  attribute in obj:
                        #print(attribute)
                        if obj[attribute] == "True" and attribute != "count":
                            customer_object.attr(attribute)
                customer_list.append(customer_object)
            return customer_list
        except:
            raise Exception("customer_get_value Failed!")
    """ Kibana-Dev """
    def _all_error_count(self):
        """ İşe ilk başlarken Kibana'dan tüm verileri  çekip işleyip gönderecektik... 
            Limitlendirmeler sonucu bucket ile gruplandırıp hepsini çektik.
            Tekrar tekrar veri çekmeyelim python işlesin. 
            Fakat Analytics tarafı  bu kadar hızlı değildi bizde, 
            yormayacak filitrelemler ile ordan az az veri çekip
            birden fazla istek yollama girişimi yaptık. 
            Gün sonunda veri çekme sitillerinin farkı bu yüzden."""
        try:                
                """ Bu yapı üzerinde iyileştirme yapılabilir!"""
                data = self.API_kbn.elastic_search(now=self.datetime_now,past=self.datetime_past)
                return self.API_kbn.server_error_count(obj=data)
        except Exception as err:
            self.logger.mnlogger.error("{}".format(err))
    
    def _msg_keyword(self):
        try:
                data = self.API_kbn.elastic_search(now=self.datetime_now,past=self.datetime_past)
                return self.API_kbn.server_msg_keyword(data)
        except Exception as err:
            self.logger.mnlogger.error("{}".format(err))
    
    def _request_keyword(self):
        try:
                data = self.API_kbn.elastic_search(now=self.datetime_now,past=self.datetime_past)
                return self.API_kbn.server_request_keyword(data)
        except Exception as err:
            self.logger.mnlogger.error("{}".format(err))
    """ Kibana-Dev """

    """ Analytics"""
    def _hit_miss(self,url):
        try:
            data = self.API_anl.analytic_search(now=self.datetime_now,past=self.datetime_past,url=url)
            return self.API_anl.analytic_search_hit_miss(data)
        except Exception as err:
            self.logger.mnlogger.error("{}".format(err))
        
    def _request_time(self,url):
        try:
            data = self.API_anl.analytic_search(now=self.datetime_now,past=self.datetime_past,url=url)
            return self.API_anl.analytic_search_request(data)
        except Exception as err:
            self.logger.mnlogger.error("{}".format(err))
            
    """ Analytics"""

    def customer_request_time_anl(self,url):
        try:
            request_time_count = self._request_time(url)
            return self.parserer_response(request_time_count)
        except:
            raise Exception("customer_request_time Faied!")

    def customer_hit_miss_anl(self,url):
        try:
            hit_miss_count = self._hit_miss(url)
            self.hit_miss_fix_it(hit_miss_count)     
            return self.parserer_response(hit_miss_count)
        except:
            raise Exception("customer_hit_miss_anl Faied!")

    def hit_miss_fix_it(self,hit_miss_list):
        """ Origin Boş geliyor """
        fixed_list = []
        try:
            for dict_hit_miss in  hit_miss_list:
                if dict_hit_miss['key'] == "":
                    dict_hit_miss['key'] = "ORIGIN"
                fixed_list.append(dict_hit_miss)
            return fixed_list
        except:
            raise Exception("hit_miss_fix_it Failed!")  
 

    def parserer_response(self,parser_obj):
        try:
            paserser_list = list() # data ayıklamada list olanları görselleştireceğiz """WebHook ile image Upload Olmuyordu..."""
            list_iter = iter(parser_obj)
            response_str= ""
            for item in list_iter:
                #print( len(list(item.values())))
                response_line= ' '.join(map(str, list(item.values())))
                response_str += "\n"
                response_str += response_line
            paserser_list.append(response_str)
            return paserser_list
        except:
            raise Exception("Failed parserer_response")

    def customer_error_count(self,url):
        try:
            error_customer =self._all_error_count()
            for host in error_customer:
                if host['key'] == url:
                    return host['doc_count']
        except:
            raise Exception(" Failed customer_error_count ")
    

    def customer_error_msg_keyword(self,url):
        try:
            msg_keyword = self._msg_keyword()
            for host in msg_keyword:
                if host['key'] == url:
                    return self.parserer_response(host['msg_keyword']['buckets'])        
        except:
            raise Exception("Failed customer_error_msg_keyword ")
    

    def customer_error_request_url(self,url):
        try:
            request_keyword = self._request_keyword()
            for host in request_keyword:
                if host['key'] == url:
                    return self.parserer_response(host['request_keyword']['buckets'])
        except:
            raise Exception("Failed customer_error_request_url")

    



    def notifyManager(self):
        customer_response_list = list()
        try:
            """ Yeni bir özellik ekleneckse Yukarıdaki class a ekledikten sonra metodunu buraya ekleki True ise metod çalışsın """
            method_dict = {
                "error_count" : self.customer_error_count,
                "message" : self.customer_error_msg_keyword,
                "request" : self.customer_error_request_url,
                "hit_miss" : self.customer_hit_miss_anl,
                "request_time" : self.customer_request_time_anl
              
            }
             
            customer_list = self.customer_get_value()
            self.logger.mnlogger.info("Customer list created!")
            for customer_obj in  customer_list:
                response_class = Customer_Response(customer=customer_obj.customer_url)
                for attribute in customer_obj.attr_list:
                    if  method_dict[attribute](customer_obj.customer_url) is None:
                        self.logger.mnlogger.warning("{} is value null. Control customer name!!!".format(attribute))
                    setattr(response_class, attribute, method_dict[attribute](customer_obj.customer_url)) # Sınıfa değerler yükleniyor...
                    self.logger.mnlogger.info("{} {}".format(attribute,"Loading customer values into the class"))
                customer_response_list.append(response_class)
                self.logger.mnlogger.info("{}  Customer values are loaded and added to the list".format(customer_obj.customer_url))
            return customer_response_list
        except:
            raise Exception("notifyManager Faied")
    
    def count_filter(self):
        
        try:
            customers_response = self.notifyManager()
            for customer in customers_response:
                if  customer.error_count is  None:
                    customer.error_count = int(0) # yanlışlıkla count değeri  False atanırsa...
                #print(customer.error_count) ### çalışmazsa önce  counter değerlerine bir bak bazen çok küçük geliyor
                if customer.error_count > int(self.customer_dict[customer.customer]['count']):
                    self.logger.mnlogger.info("{} Starting the Message Sending process...".format(customer.customer))
                    self.send_notify(customer)
                    
        except:
            raise Exception("count_filter")
    
    def send_notify(self,message):
        try:
            DataVisualization(message)    
            DataVisualization.df_table()
            data = self.message_type(message,DataVisualization)
            DataVisualization.__exit__() # Üzerinde veri kalmasın!
            webhook = os.environ['webhook_customers']
            response = requests.post(webhook, json.dumps(data))
            if response.ok:
                self.logger.mnlogger.info("{} Message sent successfully".format(message.customer))
            else:
                 self.logger.mnlogger.critical("{} {}".format(message.customer,response.status_code))
        except:
            raise Exception("Failed send_notify")
        


    def  message_type(self,message,DataVisualization):
        try:
            all_variable = [attr for attr in dir(DataVisualization) if not callable(getattr(DataVisualization, attr)) and not attr.startswith("__")]
            for vars_in_class in all_variable:
                if getattr(DataVisualization,vars_in_class) is None:
                    setattr(DataVisualization,vars_in_class,"This feature is turned off in settings")
            if  message.message is not None:
                err_msg = message.message[0]
            else:
                err_msg =  "This feature is turned off in settings"
            if self.datetime_now is None and self.datetime_past is None:
                self.datetime_now = "now"
                self.datetime_past = "yesterday" 
            msg= {
                "blocks": [
                    	{
			            "type": "divider"
		            },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": " *Customer* :  {}     *Error Count:* {}   *Time Interval*: {} <- {}  \n *HIT-MISS:* \n ``` {} ``` \n *Error Request URL Count:*  \n  ``` {}  ``` \n *Request Time*: \n ``` {} ```  \n *Error Message:* \n {}".format(message.customer,message.error_count,self.datetime_now,self.datetime_past,DataVisualization.hit_miss,DataVisualization.request,DataVisualization.request_time,err_msg)
                        }
                    },
                    	{
			        "type": "divider"
		        }
                ]
            }
            return msg
        except:
            raise Exception("sendmessage Failed!")
        finally:
            del DataVisualization  
                    



if __name__ == "__main__":
    elastic_dev = NotifyBot()
    if len(sys.argv) >= int(3):
        datetime_now = sys.argv[1:2]
        datetime_past = sys.argv[2:]
        elastic_dev.datetime_now = datetime_now[0]
        elastic_dev.datetime_past = datetime_past[0]
    elastic_dev.count_filter()