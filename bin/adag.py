#!/usr/bin/env python
# coding:utf-8

'''
auto dag
'''

# **********************************************************
# * Author        : xfzheng
# * Email         : 329472010@qq.com
# * Create time   : 2019-03-29 15:36
# * Last modified : 2019-03-29 15:36
# * Filename      : adag.py
# * Description   :
# **********************************************************

from __future__ import print_function
# import logging
import os
import time
import sys
import argparse

from build_dag import build_dag
from build_pyfile import build_pyfile
from parse_sql import parse_depends


def auto_dag():
    '''
    auto dag
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--sql-path', dest='sql_path',
                        help='需要解析的sql文件路径', nargs='?', default=".", required=True)

    parser.add_argument('-d', '--dag-id', dest='dagID',
                        help='唯一的dag id', nargs='?', default="", required=True)

    parser.add_argument('-i', '--interval', dest='interval',
                        help='设置任务运行的时间;类似crontab 每天10点[0 10 * * *]', nargs='?', default="", required=True)

    parser.add_argument('-p', '--package', dest='package',
                        help='保存的文件路径', nargs='?', default="", required=True)

    parser.add_argument('-y', '--yes', dest='ifyes',
                        help='是否需要确认', nargs='?', default="False", required=False)

    args = parser.parse_args()

    if not args.ifyes:
        inp = input("dagID={},sql_path={},interval={},package={}\n(Are you sure? Y or N):".format(
            args.dagID, args.sql_path, args.interval, args.package))
        if not inp or inp in ('N', 'n', 'no', 'NO', 'no'):
            sys.exit()

    depends_res = parse_depends(args.sql_path)

    cur_path= os.path.dirname(os.path.abspath(__file__))
    class_path = os.path.join(cur_path, "../template/class_template.tp")
    build_pyfile(depends_res, args.package, class_path)

    depends = [sub_item for item in depends_res for sub_item in item['depends']]
    object_table = set([item['obj_table'] for item in depends_res])
    depends_final = []
    for item in depends:
        it0, it1 = item.split('>>')
        if it0.strip(' ') not in object_table or it0.strip(' ') == it1.strip(' '):
            print(it0, it1)
        else:
            depends_final.append(item)

    imports = ['from {}.{} import {}'.format(args.package, item, item.replace('_','').lower().capitalize())
               for item in object_table]

    operators = [(item, item.replace('_','').lower().capitalize()) for item in object_table]
    # operators = object_table

    dag_id = '{}_{}'.format(args.dagID, time.strftime('%Y%m%d%H', time.localtime()))

    if args.package.find('.') >= 0:
        folder_p = args.package.split('.')[0]
        dag_file_name = '{}/{}'.format(folder_p, dag_id + '.py')
    else:
        dag_file_name = dag_id +'.py'

    params = {
        'imports': imports,
        'start_date':'datetime(2019,01,01)',
        'email_on_failure':'True',
        'email_on_retry':'True',
        'dag_id':dag_id,
        'interval':args.interval,
        'operators':operators,
        'depends':depends_final,
    }

    build_dag(args.package, dag_file_name, params)

if __name__ == '__main__':
    auto_dag()
