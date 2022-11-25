from pywebio.input  import *
from pywebio.output import *
import configparser
from functools import partial
from pywebio import start_server
from pywebio.session import hold, info as session_info, register_thread,run_js
import os,subprocess



@use_scope('send',clear=True)
def send_notify():
    try:
        cmd=['python3', '/usr/local/bin/base.py']
        with put_loading().style('width:4rem; height:4rem'):
            r_code, _, _ = command_run(cmd)
            if int(0) == int(r_code):
                put_text('Send Notify')
            else:
                put_text('Failed..')
    except Exception as err:
        toast('Error notify send {}'.format(err),color='error')
        



def command_run(cmd):
    try:
        process = subprocess.Popen(cmd,stderr=subprocess.PIPE,stdout=subprocess.PIPE, shell=True,preexec_fn=os.setsid)
        outs, errs = process.communicate()
        result_code = process.returncode
        return result_code, outs.decode("utf-8"), errs.decode("utf-8")
    except Exception as e:
        return 1, 'Could not execute command: {0}. Error Message: {1}'.format(cmd, str(e)), ''
    finally:
        process.kill()


def remove_customer(choice, customer):
            """ choice bilgisi buton type değerini döndürür atsak hata verir. Atmasak kullanacağız yer yok """
            try:
                p = configparser.ConfigParser()
                with open('customer.cfg', 'r+') as s:
                    p.read_file(s)
                    p.remove_section(customer)
                    s.seek(0)
                    p.write(s)
                    s.truncate()

                toast("{} removed on customer.cfg".format(customer),color="success")
            except:
                toast("Error remove customer function")
            finally:
                remove('table')
                config_read()


def check_customer(data):
    try:
        customer = configparser.RawConfigParser()
        customer.read('customer.cfg')
        if '{}'.format(data['customer']) in customer:
            return ('customer','This customer is registered!!!')
        if data['count'] <= 0:
            return ('count', 'count cannot be negative!')
    except Exception as err:
        toast("{}".format(err),color='warning')



@use_scope('table',clear=True)
def config_read():
            config_list = list()
            customer = configparser.RawConfigParser()
            customer.read('customer.cfg')
            for customer_detail in customer._sections:
                config_item = customer._sections[customer_detail]
                config_item['Customer'] = customer_detail
                config_item['operation'] = put_buttons(['Remove'], onclick=partial(remove_customer,customer=customer_detail))
                config_list.append(config_item)

            put_table(config_list,header=["Customer","count","request_time","hit_miss","request","error_count","operation"])

@use_scope('form',clear=True)
def customer_add():
            try:
                data =  input_group("Customer Form",
                            [input('Domain',name='customer',required=True),
                            input('Count',name='count',type=NUMBER,required=True),
                            radio('ERROR COUNT', name='error_count',options=["True","False"],required=True),
                            radio('HIT MISS RATIO',name='hit_miss',options=["True","False"],required=True),
                            radio('ERROR MESSAGE',name='message',options=["True","False"],required=True),
                            radio('REQUEST URL',name='request',options=["True","False"],required=True),
                            radio('REQUEST TIME',name='request_time',options=["True","False"],required=True)],validate=check_customer)

                config = configparser.RawConfigParser()
                config.read('customer.cfg')
                config.add_section(data['customer'])
                config.set(data['customer'], 'error_count', data['error_count'])
                config.set(data['customer'], 'message', data['message'])
                config.set(data['customer'], 'request',data['request'])
                config.set(data['customer'], 'hit_miss',data['hit_miss'])
                config.set(data['customer'],'request_time',data['request_time'])
                config.set(data['customer'], 'count',data['count'])
                with open('customer.cfg', 'w') as configfile:
                    config.write(configfile)
                    toast("{} client written to file".format(data['customer']),color='success')
            except:
                toast("client could not write to file",color='warning')
                raise Exception("customer form failed")

@use_scope(clear=True)
def button_manager(btn):

    manager_list = {
        'Customer Table' : config_read,
        'Customer Add From': customer_add,
        'Send Notify': send_notify
    }

    manager_list[btn]()



def main():
            remove('send')
            put_markdown('## Customer Base Notify')
            put_markdown('> Edit Customer File WebIO')
            put_buttons(['Customer Add From','Customer Table','Send Notify'],onclick=button_manager)
            hold()




if __name__ == '__main__':
    start_server(main, debug=True, port=8080, cdn=False)