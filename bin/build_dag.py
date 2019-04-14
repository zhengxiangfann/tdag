#!/usr/bin/env python
# coding:utf-8
'''
build_pyfile
'''
# **********************************************************
# * Author        : xfzheng
# * Email         : 329472010@qq.com
# * Create time   : 2019-03-29 11:27
# * Last modified : 2019-03-30 15:26
# * Filename      : build_dag.py
# * Description   :
# **********************************************************

# import time

# from datetime import datetime


import os
import sys
from shutil import copyfile

from jinja2 import FileSystemLoader, Environment

def copy_base_class(obj_path):

    if obj_path.find('.') >= 0:
        path = '/'.join(obj_path.split('.'))
    else:
        path = obj_path

    cur_path= os.path.dirname(os.path.abspath(__file__))
    src_file = os.path.join(cur_path, '../base/BaseDag.py')
    obj_file = os.path.join(cur_path, '{}/BaseDag.py'.format(path))

    try:
        copyfile(src_file, obj_file)
    except IOError as ex:
        print('Unable to copy file. %s' % src_file)
        sys.exit(1)


def build_dag(dag_path, dag_filename, args, template_path='../template'):
    """
    # 可以当参数传递进去
    filename = 'test.py'
    dag_id = 'test_{}'.format(time.strftime('%Y%m%d%H', time.localtime()))
    interval = '0 10 * * *'
    imports = ["from xf.abc import abc", "from xf.abc import bcd",
               "from xf.abc import dee",
               "from xf.abc import efg"
               ]
    operators = ["abc", "bcd", "dee", "efg"]
    depends = ["abc >> bcd >>", "dee >> efg"]

    args = {"imports": imports,
            "start_date": "datetime(2019,03,28)",
            "email_on_failure": "True",
            "email_on_retry": "True",
            "dag_id": dag_id,
            "interval": interval,
            "operators": operators,
            "depends": depends}

    """

    copy_base_class(dag_path)
    dag_filepath = os.path.join(dag_path, dag_filename)
    loader = FileSystemLoader(template_path, encoding='utf-8')
    env = Environment(loader=loader)
    temp = env.get_template('dag_template.tp')
    temp.stream(**args).dump(dag_filename)
