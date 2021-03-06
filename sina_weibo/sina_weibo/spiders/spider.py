from scrapy.exceptions import CloseSpider
from scrapy import Request,Spider
from time import sleep
from ..items import *
import redis
import json
import re
import logging
import traceback



class WeiboSpider(Spider):
    name = 'weibo'
    start_urls = [
                  'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_1826792401&type=all&since_id={}',
                  'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_followers_-_1826792401&type=all&page={}'
                  ]
    follower_api = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_{}&type=all&since_id=0'
    follow_api = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_followers_-_{}&type=all&page=0'
    follower_max_page = 250
    follow_max_page = 10
    r = redis.Redis(host='localhost', port=6379, password='123456789')
    logger = logging.getLogger(__name__)

    def start_requests(self):
        for i in range(self.follower_max_page+1):
            yield Request(self.start_urls[0].format(i),callback=self.parse_follower)
        for j in range(self.follow_max_page+1):
            yield Request(self.start_urls[1].format(j),callback=self.parse_follow)

    def parse_follower(self, response):
        print(response.text)
        if not response.status == 200:
           sleep(300)
        try:
            resp = json.loads(response.text)
            if resp.get('ok') == 1:
                item = UserItem()
                datas = resp['data'].get('cards')
                follows_ids = []
                relationship = {}
                for data in datas[0].get('card_group'):
                    item['user_info'] = data.get('user')
                    print(item)
                    yield item
                    id = data.get('user').get('id')
                    follows_ids.append(id)
                relationship['followers_ids'] = follows_ids
                relationship['user_id'] = re.findall('_(\d+)&type', response.url)[0]
                relationship['source_url'] = response.url
                item['relationship'] = relationship
                yield item
                for data in datas[0].get('card_group'):
                    if data:
                        id = data.get('user').get('id')
                        followers_count = data.get('user').get('followers_count')
                        follow_count = data.get('user').get('follow_count')
                        self.logger.info('id %s ,粉丝 %s, 关注 %s' % (id,followers_count,follow_count))
                        added = self.r.sadd('user_id',id)
                        if followers_count > 50 and follow_count > 20:
                                yield Request(self.follower_api.format(id),callback=self.parse_follower_page)
                                yield Request(self.follow_api.format(id),callback=self.parse_follow_page)
            else:
                print(resp)
                return
        except Exception as e:
            traceback.print_exc()
            print(e,json.loads(response.text),response.url)


    def parse_follower_page(self,response):
        if not response.status == 200:
           sleep(300)
        try:
            resp = json.loads(response.text)
            if resp.get('ok') == 1:
                if response.meta.get('page'):
                    i = response.meta['page']
                    self.logger.debug('i:%s'% i)
                    meta = {'page': i+1}
                    url = str(response.url).replace('&since_id={}'.format(i),'&since_id={}'.format(i+1))
                    yield Request(url,callback=self.parse_follower)
                    yield Request(url,callback=self.parse_follower_page,meta=meta)
                else:
                    i = 1
                    meta = {'page': i}
                    url = str(response.url).replace('&since_id=0', '&since_id={}'.format(i))
                    yield Request(url,callback=self.parse_follower,dont_filter=True)
                    yield Request(url,callback=self.parse_follower_page,meta=meta,dont_filter=True)
            else:
                print(resp)
                return
        except Exception as e:
            repr(e)



    def parse_follow(self,response):
        if not response.status == 200:
           sleep(300)
        try:
            resp = json.loads(response.text)
            if resp.get('ok') == 1:
                item = UserItem()
                datas = resp['data'].get('cards')
                follows_ids = []
                relationship = {}
                for data in datas[-1].get('card_group'):
                    item['user_info'] = data.get('user')
                    yield item
                    id = data.get('user').get('id')
                    follows_ids.append(id)
                relationship['follows_ids'] = follows_ids
                relationship['user_id'] = re.findall('_(\d+)&type', response.url)[0]
                relationship['source_url'] = response.url
                item['relationship'] = relationship
                yield item
                for data in datas[-1].get('card_group'):
                    if data:
                        id = data.get('user').get('id')
                        followers_count = data.get('user').get('followers_count')
                        follow_count = data.get('user').get('follow_count')
                        self.logger.info('id %s ,粉丝 %s, 关注 %s' %(id,followers_count,follow_count))
                        added = self.r.sadd('user_id',id)
                        if followers_count > 50 and follow_count > 20:
                            yield Request(self.follower_api.format(id), callback=self.parse_follower_page)
                            yield Request(self.follow_api.format(id), callback=self.parse_follow_page)
            else:
                print(resp)
                return
        except Exception as e:
            traceback.print_exc()
            print(e,json.loads(response.text),response.url)


    def parse_follow_page(self, response):
        if not response.status == 200:
           sleep(300)
        try:
            resp = json.loads(response.text)
            if resp.get('ok') == 1:
                if response.meta.get('page'):
                    i = response.meta['page']
                    self.logger.debug('i:%s'% i)
                    meta = {'page': i + 1}
                    url = str(response.url).replace('&page={}'.format(i), '&page={}'.format(i + 1))
                    yield Request(url, callback=self.parse_follow_page, meta=meta)
                    yield Request(url, callback=self.parse_follow)
                else:
                    i = 1
                    meta = {'page': i}
                    url = str(response.url).replace('&page=0', '&page={}'.format(i))
                    yield Request(url, callback=self.parse_follow_page, meta=meta)
                    yield Request(url, callback=self.parse_follow)
            else:
                print(resp)
                return
        except Exception as e:
            repr(e)
