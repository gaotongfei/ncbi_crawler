# coding: utf-8
import re
import MySQLdb
import requests
from bs4 import BeautifulSoup
from warnings import filterwarnings
import time

def get_soup(response):
    return BeautifulSoup(response.text)


def find_by_class(classname):
    return soup.find_all('div', classname)


def get_authors(classname):
    check_authors = find_by_class(classname)[0].get_text()
    serach_num = re.search(r'\d+', check_authors)
    if check_authors.startswith("[No authors listed]"):
        authors = "No authors listed"
        return authors
    elif serach_num is None:
        authors = check_authors
        return authors
    else:
        authors = check_authors.replace(serach_num.group(0), "")
        return authors


def get_departments(classname):
    check_departments = find_by_class(classname)
    if not check_departments:
        departments = None
        return departments
    else:
        departments = find_by_class(classname)[0].contents[1].get_text()
        return departments


def get_abstract(classname):
    check_abstract = find_by_class(classname)
    if not check_abstract:
        abstract = None
        return abstract
    else:
        abstract = find_by_class(classname)[0].get_text()
        return abstract


def get_keywords(classname):
    check_keyword = find_by_class(classname)
    if not check_keyword:
        keywords = None
        return keywords
    else:
        keywords_with_KEYWORDS = find_by_class(classname)[0].get_text()
        the_word_KEYWORDS = re.search(r'\KEYWORDS: ', keywords_with_KEYWORDS)
        keywords = keywords_with_KEYWORDS.replace(the_word_KEYWORDS.group(0), '')
        return keywords

filterwarnings('ignore', category=MySQLdb.Warning)
# 链接数据库
conn = MySQLdb.connect(host="", user="", passwd="")
# 创建游标
cursor = conn.cursor()
conn.set_character_set('utf8')
cursor.execute('SET NAMES utf8;')
cursor.execute('SET CHARACTER SET utf8;')
cursor.execute('SET character_set_connection=utf8;')

# 创建数据库
cursor.execute("create database if not exists ncbi")
# 选择数据库
conn.select_db('ncbi')
# sql语句
create_table_sql = (
    "CREATE TABLE if not exists content("
    "pmid VARCHAR(10) PRIMARY KEY,"
    "mgz_name VARCHAR (50),"
    "pub_time VARCHAR (20),"
    "volume VARCHAR (10),"
    "epub_time VARCHAR (20),"
    "title TEXT,"
    "authors VARCHAR (100),"
    "departments TEXT,"
    "abstract TEXT,"
    "keywords VARCHAR(100)"
    ")"
)

insert_sql = "INSERT IGNORE INTO content (pmid,mgz_name,pub_time,volume,epub_time,title,authors,departments,abstract,keywords) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

cursor.execute(create_table_sql)

with open('error.log.txt', 'r') as f:
    for line in f:
        if re.match('\page', line) is None:
            pmid = line.split()[0].strip("\n")
            url = "http://www.ncbi.nlm.nih.gov/pubmed/" + pmid
            headers = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36"}
            try:
                print pmid + " is trying to get requests"
                r = requests.get(url, headers=headers, timeout=6)
                soup = get_soup(r)
                ########################################
                # head部分单独处理
                # content中包含“杂志名称”，“出版时间”，“电子出版时间”，“卷（目）”
                head_content = str(find_by_class('cit')[0].get_text())
                # 以.切割
                content_splited = head_content.split('.')
                # 杂志名称
                mgz_name = content_splited[0]
                # 电子出版日期
                epub_time = content_splited[len(content_splited)-2]
                # 以;切割
                pub_time_and_volume = str(content_splited[1]).split(';')
                # 出版日期
                pub_time = pub_time_and_volume[0]
                # 卷（目）
                volume = pub_time_and_volume[1]
                # 标题
                title = find_by_class('cit')[0].next_sibling.get_text()
                ########################################

                # 作者
                authors = get_authors('auths')

                # 单位
                departments = get_departments('afflist')

                # 摘要
                abstract = get_abstract('abstr')

                # 关键词
                keywords = get_keywords('keywords')
            except:
                error_log = open('error.log', 'a')
                error_log.write(pmid+'\n')
                error_log.close()
                print pmid + " get an error,the pmid has been recorded into the file error.log"
                continue

            cursor.execute(insert_sql, (pmid, mgz_name, pub_time, volume, epub_time, title, authors, departments, abstract, keywords))
        conn.commit()

cursor.close()
conn.close()

