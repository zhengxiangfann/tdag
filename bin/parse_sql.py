#!/usr/bin/env python
# coding:utf-8

# **********************************************************
# * Author        : xfzheng
# * Email         : 329472010@qq.com
# * Create time   : 2019-03-29 18:33
# * Last modified : 2019-03-29 18:33
# * Filename      : parse_sql.py
# * Description   :
# **********************************************************

import re
import ply.lex as lex
import sqlparse


def read_sql(sql_path):
    with open(sql_path, 'r') as fr:
        return fr.read()

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


def parse_depends(sql_path):

    sql_blocks = split_sql(read_sql(sql_path))

    depends_res = []
    for sql_str in sql_blocks:
        table_names = __extract_table_name_from_sql(sql_str)
        depends = []
        if table_names:
            for tb in table_names[1]:
                depends.append('{} >> {}'.format(tb, table_names[0]))
            sql_str_format = sqlparse.format(sql_str, reindent=True, keyword_case='upper')
            depends_res.append({'depends': depends, 'sql': sql_str_format, 'obj_table': table_names[0]})
    return depends_res


if __name__ == '__main__':

    sql_path = "a.sql"
    print(parse_depends(sql_path))
