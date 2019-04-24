#!/usr/bin/env python
# coding:utf-8

# **********************************************************
# * Author        : xfzheng
# * Email         : 329472010@qq.com
# * Create time   : 2019-03-29 10:20
# * Last modified : 2019-03-29 10:20
# * Filename      : BaseDag.py
# * Description   :
# **********************************************************

import sys
import subprocess

class BaseDag(object):


    def __init__(self, conf=None):
        self.conf = {'host':'192.268.1.100', 'port':10000, 'username':'zhengxf', 'passwd':'zhengxf'}
        self.cmd = None

    def _build_cmd(self):
        self.cmd = 'beeline -u "jdbc:hive2://{host}:{port}" -n {username} -p {passwd} -e"{sql}"'.format(
                host=self.conf['host'],
                port=self.conf['port'],
                username=self.conf['username'],
                passwd=self.conf['passwd'],
                sql=self.sql
        )
        print('cmd %s' % self.cmd)

    def run_command(self):
        self.sql = """select * from belle_jw.pro_month_sku_sal_18 limit 1"""
        self.call()

    def call(self):
        self._build_cmd()
        process = subprocess.Popen(self.cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        returncode = process.wait()
        if returncode == 0:
            err = process.stderr.read()
            try:
                err_list = err.split('\n')
                for err_str in err_list :
                    if err_str.find('Error', 0) >= 0 or err_str.find('fail') >=0:
                        print(err_str)
                        sys.exit(1)
            except:
                print('============')
            print('Success')
            return True
        else:
            print('retCode: ', returncode)
            sys.exit(1)

    def catch_error(self):
        pass

    def signal(self):
        pass


if __name__ == '__main__':
    BaseDag().run_command()
