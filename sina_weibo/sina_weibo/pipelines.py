# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
import logging


class SinaWeiboPipeline(object):

    logger = logging.getLogger(__name__)

    def __init__(self):
        try:
            url = 'localhost'
            port = 27017
            connection = pymongo.MongoClient(url,port)
            self.db = 'weibo'
            self.col_1 = 'user_info'
            self.col_2 = 'user_relationship'
            db = connection[self.db]
            self.collection_1 = db[self.col_1]
            self.collection_2 = db[self.col_2]
        except Exception as e:
               self.logger.info('%s' % e)

    def process_item(self, item, spider):
        try:
            if item.get('relationship') :
                self.collection_2.insert(dict(item['relationship']))
            elif item.get('user_info'):
                self.collection_1.insert(dict(item))
            return item
        except Exception as e:
            self.logger.info('%s' % e)

            # def __init__(self):
    #     conn = Connection(host='localhost',port=9090)
    #     self.table = conn.table('sina_weibo')
    #     print(conn)
    #
    # def process_item(self, item, spider):
    #     try:
    #         user_info = item['user_info']
    #         self.table.put(md5(user_info).hexdigest(),
    #                        {'cf1:user_info': user_info})
    #         print('写入hbase成功!!')
    #         return item
    #     except Exception as e:
    #         print(e)




