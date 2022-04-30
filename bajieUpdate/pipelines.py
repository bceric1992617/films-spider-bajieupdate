# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql 
import scrapy
import os
import shutil
import time
from scrapy.pipelines.images import ImagesPipeline
from bajieUpdate.settings import IMAGES_STORE
from bajieUpdate.getFilmMsg import getFilmMsg


class bajieUpdatePipeline: 
    def __init__(self):
        self.db = pymysql.connect(
            host="localhost",
            port=3306, 
            db='films', 
            user='root', 
            passwd='123321', 
            charset='utf8'
        )
        self.db_cur = self.db.cursor()

    def process_item(self, item, spider):
        item['filmId'] = item['id']
        if bool(item['id']) == False:
            #获取国家信息
            countryList = getFilmMsg(self.db_cur).getCountry()
            #获取分类信息
            filmtypesList = getFilmMsg(self.db_cur).getFilmtypes()
            #插入电影信息
            self.insertFilm(item,countryList,filmtypesList)
            #获取上一条插入的Id
            item['filmId'] = self.getLastInsertId()[0][0]

        #插入播放连接
        self.insertPlayAddr(item)
        self.updateTime(item)
        self.db.commit() 
        return item

      

    def getLastInsertId(self):   
        self.db_cur.execute("select last_insert_id();")
        return self.db_cur.fetchall()


    #插入电影表
    def insertFilm(self,item,countryList,filmtypesList):
        sql_film_info = """
            insert into films(filmsName,tags,countries,years,directors,stars,douban,filmLength,language,content,mainPicAddr,createTime,updateTime,isDel,types) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        try:
            country = countryList[item['countries']][0]
        except:
            country = 1000

        try:
            tags = filmtypesList[item['tags']][0]
        except:
            if item['tags'] == '纪录片':
                tags = 105
            elif item['tags'] == '台湾剧':
                tags = 107
            elif item['tags'] == '香港剧':
                tags = 101
            elif item['tags'] == '国产剧':
                tags = 100
            elif item['tags'] == '韩国剧':
                tags = 104
            elif item['tags'] == '日本剧':
                tags = 103
            elif item['tags'] == '欧美剧':
                tags = 102
            elif item['tags'] == '海外剧':
                tags = 106
            else:
                tags = 1000

        self.db_cur.execute(sql_film_info, [
            item['filmsName'],
            tags,
            country,
            item['years'],
            item['directors'],
            item['stars'],
            item['douban'],
            item['filmLength'],
            item['language'],
            item['content'],
            item['picAddr'],
            item['createTime'],
            item['updateTime'],
            '0',
            item['videoType']
        ])
       


    #插入播放信息
    def insertPlayAddr(self,item):
        sql = """
            insert into bajiecaiji(filmsId,playAddrName,playAddr,linkType,createTime,updateTime) values(%s,%s,%s,%s,%s,%s);
        """

        try:
            for index in range(len(item['playAddr']['yun']['addr'])):
                self.db_cur.execute(sql, [
                    item['filmId'],
                    item['playAddr']['yun']['name'][index],
                    item['playAddr']['yun']['addr'][index],
                    1,
                    item['createTime'],
                    item['updateTime']
                ])
        except:
            print('没有yun连接。。。' * 2)

        try:
            for index in range(len(item['playAddr']['m3u8']['addr'])):
                self.db_cur.execute(sql, [
                    item['filmId'],
                    item['playAddr']['m3u8']['name'][index],
                    item['playAddr']['m3u8']['addr'][index],
                    2,
                    item['createTime'],
                    item['updateTime']
                ])
        except:
            print('没有m3u8连接。。。' * 2)
            

    #更新视频更新时间
    def updateTime(self,item):
        sql = """
            update films set updateTime=%s where filmsId=%s;
        """

        self.db_cur.execute(sql, [
            int(round(time.time())),
            item['filmId']
        ])

        

