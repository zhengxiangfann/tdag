#!/usr/bin/env python
# coding:utf-8


import sys
import tempfile
import subprocess


class BaseDag(object):

    def __init__(self, conf=None):
        self.conf = {'host': '10.240.20.20', 'port': 10000,
                     'username': 'zheng.xf', 'passwd': 'zheng.xf'}
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

#    def call(self):
#        self._build_cmd()
#        process = subprocess.Popen(self.cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#        returncode = process.wait()
#        if returncode == 0:
#            err = process.stderr.read()
#            try:
#                err_list = err.split('\n')
#                for err_str in err_list :
#                    if err_str.find('Error', 0) >= 0 or err_str.find('fail') >=0:
#                        print(err_str)
#                        sys.exit(1)
#            except:
#                print('============')
#            print('Success')
#            return True
#        else:
#            print('retCode: ', returncode)
#            sys.exit(1)
#

    def call(self):
        out_temp = tempfile.TemporaryFile(mode='w+')
        fileno = out_temp.fileno()

        self._build_cmd()
        process = subprocess.Popen(
            self.cmd, shell=True, stdout=fileno, stderr=fileno)
        returncode = process.wait()
        if returncode == 0:
            out_temp.seek(0)
            rt = out_temp.read()
            try:
                err_list = rt.strip().split('\n')
                for err_str in err_list:
                    if err_str.find('Error', 0) >= 0 or err_str.find('ERROR') >= 0:
                        print(err_str)
                        sys.exit(1)
            except:
                print('============')
            print('Success')
            return 0
        else:
            print('retCode: ', returncode)
            sys.exit(1)

    def catch_error(self):
        pass

    def signal(self):
        pass


if __name__ == '__main__':
    BaseDag().run_command()
