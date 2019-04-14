#!/usr/bin/env python
# coding:utf-8

# **********************************************************
# * Author        : xfzheng
# * Email         : 329472010@qq.com
# * Create time   : 2019-03-29 11:27
# * Last modified : 2019-03-29 18:39
# * Filename      : build_pyfile.py
# * Description   :
# **********************************************************
import os


def build_pyfile(depends_res, package, template_path='../template/class_template.tp'):
    '''
        template_path = '../template/class_template.tp'
        filename = 'pro_month_sal_rank_final_18.py'
        class_name = filename.replace('_', '').split('.')[0].capitalize()
        sql = "select * from t wher id=123"
    '''

    if package.find('.') >= 0:
        folders = package.split('.')
        path = '/'.join(folders)
    else:
        path = package

    if not os.path.exists(path):
        os.makedirs(path)
        paths = ''
        for p in path.split('/'):
            paths += p + '/'
            with open('%s/%s' % (paths, '__init__.py'), 'w') as fw:
                pass

    for item in depends_res:
        file_name = '{}.py'.format(item['obj_table'])
        class_name = file_name.replace('_', '').lower().split('.')[0].capitalize()
        sql = item['sql']

        with open(template_path, 'r') as fr, open('%s/%s' % (path, file_name), "w") as fw:
            fw.write(fr.read().format(classname=class_name, sql=sql))

if __name__  == '__main__':
    build_pyfile([], 'zxf.abc.zmm.zbt')
