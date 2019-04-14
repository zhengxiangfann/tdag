## 使用说明 ##
1. python3 adag.py -s=test.sql -d=pro_plan -i="0 10 * * *" -p=zxf
2. -s --sql-path sql路径
3. -d --dag-id 
4. usage: adag.py [-h] -s [SQL_PATH] -d [DAGID] -i [INTERVAL] -p [PACKAGE]
5. adag.py: error: the following arguments are required: -s/--sql-path, -d/--dag-id, -i/--interval, -p/--package

