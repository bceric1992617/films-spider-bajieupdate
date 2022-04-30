# -*- coding: utf-8 -*-
import scrapy
import json
import os
import time
import datetime
import sys
import re
import pymysql
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from bajieUpdate.items import bajieUpdateItem




class bajieUpdate(CrawlSpider):
    db = pymysql.connect(
        host="localhost",
        port=3306, 
        db='films', 
        user='root', 
        passwd='123321', 
        charset='utf8'
    )
    db_cur = db.cursor()
    
    name = 'bajieUpdate'
    allowed_domains = [
        'bajiecaiji.com'
    ]

    start_urls = []
    filmUrls = [
        'http://cj.bajiecaiji.com',
        'http://cj.bajiecaiji.com/?m=vod-index-pg-$.html',
    ]

    pageNum = 2
    i = 1
    while i <= pageNum:
        if i == 1:
            start_urls.append(filmUrls[0])
        else:
            start_urls.append(filmUrls[1].replace('$', str(i), 1))
        i += 1

    # http://cj.bajiecaiji.com/?m=vod-type-id-1.html
    # http://cj.bajiecaiji.com/?m=vod-type-id-1-pg-2.html
    rules = (
        # Rule(LinkExtractor(allow=('\?m=vod-detail-id-210532\.html')), callback = 'parseFilmSpider'),
        Rule(LinkExtractor(allow=('\?m=vod-detail-id-\d+\.html')), callback = 'parseFilmSpider'),
    )

    def parseFilmSpider(self, response):
        item = bajieUpdateItem()
        item['videoType'] = 400
        item['tags'] = response.xpath("//div[@class='videoDetail']/li[6]/div[1]/text()").extract()[0].split(': ')[1].replace(' ','')
        if item['tags'].find('片') > -1 or item['tags'] == '微电影':
            item['videoType'] = 400
        elif item['tags'].find('剧') > -1 or item['tags'] == '纪录片':
            item['videoType'] = 401
        elif item['tags'].find('综艺') > -1:
            item['videoType'] = 402
        elif item['tags'].find('动漫') > -1:
            item['videoType'] = 403

        item['playAddr'] = {
            'yun' : {
                'name' : [],
                'addr' : []
            },
            'm3u8' : {
                'name' : [],
                'addr' : []
            }
        }
        playAddr = response.xpath("//div[@class='movievod']/ul/li/input/@value").extract()
        playAddr.pop()
        playAddrArr = self.list_split(playAddr,int(len(playAddr) / 2))
        for i in range(len(playAddrArr[0])):
            addrArr = playAddrArr[0][i].split('$')
            item['playAddr']['yun']['name'].append(addrArr[0])
            item['playAddr']['yun']['addr'].append(addrArr[1])

        for i in range(len(playAddrArr[1])):
            addrArr = playAddrArr[1][i].split('$')
            item['playAddr']['m3u8']['name'].append(addrArr[0])
            item['playAddr']['m3u8']['addr'].append(addrArr[1])

        name = response.xpath("//div[@class='videoDetail']/li[1]/text()").extract()[0].split(': ')[1]
        item['filmsName'] = re.sub(r"\[|]|\"|”|\'|’|\s|（|）|(|)",'',name)   
        self.db_cur.execute("select filmsId from films where filmsName= '"+ item['filmsName'] +"' limit 1;")
        filmMsg = self.db_cur.fetchone()
        item['createTime'] = int(round(time.time()))
        item['updateTime'] = int(round(time.time()))

        if bool(filmMsg):
            item['id'] = filmMsg[0]
            item = self.linkProccess(response,item)

        else:   
            item['id'] = 0
            item['directors'] = response.xpath("//div[@class='videoDetail']/li[5]/text()").extract()[0].split(': ')[1]
            item['stars'] = response.xpath("//div[@class='videoDetail']/li[4]/text()").extract()[0].split(': ')[1].replace('更多...','').strip('、')
            years = response.xpath("//div[@class='videoDetail']/li[8]/div[2]/text()").extract()[0].split(': ')[1]
            item['years'] = time.mktime(time.strptime(years, "%Y"))
            item['language'] = response.xpath("//div[@class='videoDetail']/li[7]/div[1]/text()").extract()[0].split(': ')[1]
            item['content'] = response.xpath("//div[@class='movievod']/p[2]/text()").extract()
            item['countries'] = response.xpath("//div[@class='videoDetail']/li[7]/div[2]/text()").extract()[0].split(': ')[1]
            item['picAddr'] = response.xpath("//div[@class='videoPic']/img/@src").extract()[0]
            item['douban'] = 0
            item['filmLength'] = 0

        yield item


    def list_split(self, items, n):
        return [items[i:i+n] for i in range(0, len(items), n)]


    #连接差集处理
    def linkProccess(self, response, item):
        self.db_cur.execute("select playAddr from bajiecaiji where linkType=2 and filmsId=" + str(item['id']))
        playM3u8LinkMsg = [i for j in self.db_cur.fetchall() for i in j]
        self.db_cur.execute("select playAddr from bajiecaiji where linkType=1 and filmsId=" + str(item['id']))
        playKuyunLinkMsg = [i for j in self.db_cur.fetchall() for i in j]
        self.db_cur.execute("select playAddrName from bajiecaiji where linkType=2 and filmsId=" + str(item['id']))
        playM3u8NameMsg = [i for j in self.db_cur.fetchall() for i in j]
        self.db_cur.execute("select playAddrName from bajiecaiji where linkType=1 and filmsId=" + str(item['id']))
        playKuyunNameMsg = [i for j in self.db_cur.fetchall() for i in j]
        item['playAddr']['m3u8']['addr'] = list(set(item['playAddr']['m3u8']['addr']).difference(set(playM3u8LinkMsg)))
        item['playAddr']['yun']['addr'] = list(set(item['playAddr']['yun']['addr']).difference(set(playKuyunLinkMsg)))
        item['playAddr']['m3u8']['name'] = list(set(item['playAddr']['m3u8']['name']).difference(set(playM3u8NameMsg)))
        item['playAddr']['yun']['name'] = list(set(item['playAddr']['yun']['name']).difference(set(playKuyunNameMsg)))
        return item





      