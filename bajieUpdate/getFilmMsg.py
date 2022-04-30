# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

#电影分类和国家信息
class getFilmMsg(object):
    def __init__(self, db_cur):
        self.db_cur = db_cur

    #获取电影类型数据 
    def getFilmtypes(self):
        sql_filmtypesList = """
            select * from filmtypesList
        """ 
        self.db_cur.execute(sql_filmtypesList)
        filmtypes = self.db_cur.fetchall()
        filmtypesList = {}
        for index in range(len(filmtypes)):
            filmtypesList[filmtypes[index][1]] = filmtypes[index]
        return filmtypesList
        

    #获取国家数据
    def getCountry(self):
        sql_country = """
            select * from country
        """ 
        self.db_cur.execute(sql_country)
        country = self.db_cur.fetchall()
        countryList = {}
        for index in range(len(country)):
            countryList[country[index][1]] = country[index]
        return countryList


