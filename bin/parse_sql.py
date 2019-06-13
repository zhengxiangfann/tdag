#!/usr/bin/env python
# coding:utf-8
import os
import sys
import re
import ply.lex as lex
import sqlparse
import string
import random


def read_sql(sql_path):
    print('sql_path', sql_path)
    sql = ""
    for file1 in sql_path:
        with open(file1, 'r') as fr:
            sql += fr.read()
    return sql


def split_sql(sql):
    sql = trimsql(sql)
    sql_blocks = []
    sp = re.compile('drop')
    res = sp.split(sql)
    for table in res:
        if table and table.strip(' ').find('table') == 0:
            sql_blocks.append('drop %s' % table)
    return sql_blocks


def trimsql(sql_str):

    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)
    # remove whole line -- and # comments
    lines = [line for line in q.splitlines(
    ) if not re.match("^\s*(--|#)", line)]
    # remove trailing -- and # comments
    q = "\n".join([re.split("--|#", line)[0] for line in lines])

    return q


def __extract_table_name_from_sql(sql_str):
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)
    # remove whole line -- and # comments
    lines = [line for line in q.splitlines(
    ) if not re.match("^\s*(--|#)", line)]
    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])
    #
    # split on blanks, parens and semicolons
    tokens = re.split(r"[\s)(;]+", q)

    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).

    result = []
    get_next = False
    for token in tokens:
        if get_next:
            if token.lower() not in ["", "select"]:
                result.append(token)
            get_next = False
        get_next = token.lower() in ["from", "join", "table"]
    if result:

        result = [r.split('.')[1] if len(r.split('.'))
                  == 2 else r for r in result]

        ctable, rtable = result[0], result[1:]
        if ctable and rtable and ctable in rtable:
            rtable.remove(ctable)
        return ctable, list(set(rtable))
    return


# def parse_depends(sql_path):
#
#    sql_blocks = split_sql(read_sql(sql_path))
#
#    depends_res = []
#    for sql_str in sql_blocks:
#        table_names = __extract_table_name_from_sql(sql_str)
#        depends = []
#        if table_names:
#            for tb in table_names[1]:
#                depends.append('{} >> {}'.format(tb, table_names[0]))
#            sql_str_format = '\n'.join([line for line in sqlparse.format(sql_str, reindent=False, keyword_case='upper').split('\n') if line])
#            depends_res.append({'depends': depends, 'sql': sql_str_format, 'obj_table': table_names[0]})
#    return depends_res
#

def list_sqlfile(sql_path, ext='.sql'):
    file_paths = []
    for root, dirs, files in os.walk(sql_path, topdown=False):
        for name in files:
            if name.endswith(ext):
                file_paths.append(os.path.join(root, name))
    return file_paths


var_map = dict()


def parse_depends(sql_path):

    reg = re.compile(r"\{[^{}]*\}")

    if os.path.exists(sql_path):
        if sql_path.endswith('/'):
            file_paths = list_sqlfile(sql_path)
        elif sql_path.endswith('.sql'):
            file_paths = [sql_path, ]
    else:
        print('sql file path not exists')
        sys.exit(0)

    print('sql_paths', file_paths)
    sql_blocks = split_sql(read_sql(file_paths))

    depends_res = []
    for sql_str in sql_blocks:
        table_names = __extract_table_name_from_sql(sql_str)
        depends = []
        if table_names:
            for tb in table_names[1]:
                if "{" in tb and "}" in tb:
                    #var_map.update({v[1:-1]:'' for  v in reg.findall(tb)})
                    for v in reg.findall(tb):
                        if v[1:-1] not in var_map:
                            ran_str = ''.join(random.sample(
                                string.ascii_letters + string.digits, 8))
                            var_map[v[1:-1]] = ran_str
                if "{" in table_names[0] and "}" in table_names[0]:
                    #var_map.update({v[1:-1]:'' for  v in reg.findall(table_names[0])})
                    for v in reg.findall(table_names[0]):
                        if v[1:-1] not in var_map:
                            ran_str = ''.join(random.sample(
                                string.ascii_letters + string.digits, 8))
                            var_map[v[1:-1]] = ran_str

                tb1 = tb.format(**var_map)
                tb_name1 = table_names[0].format(**var_map)

                depends.append('{} >> {}'.format(tb1, tb_name1))
            sql_str_format = '\n'.join([line for line in sqlparse.format(
                sql_str, reindent=False, keyword_case='upper').split('\n') if line])
            depends_res.append(
                {'depends': depends, 'sql': sql_str_format, 'obj_table': tb_name1})
    return depends_res


if __name__ == '__main__':

    sql_path = "a.sql"
    print(parse_depends(sql_path))
