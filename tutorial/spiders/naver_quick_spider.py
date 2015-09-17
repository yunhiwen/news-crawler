# -*- coding: utf-8 -*-

import sys, traceback
import re
import time
import json
import urllib
import time
from datetime import datetime, timedelta
from urlparse import urlparse, parse_qs
import scrapy
from tutorial.items import NaverArticleItem, NaverCommentItem
import MySQLdb

class NaverQuickSpider(scrapy.Spider):
    name = 'Naver_quick'
    allowed_domains = ['naver.com']
    start_urls = []

    s_date = ''
    e_date = ''
    c_date = ''
    page_cnt = 1
    dont_filter = False
    agency_list = []
    '''
    Constructor
    '''
    def __init__(self, start_date = '', end_date = '',check_date = '', *args, **kwargs):
        self.s_date = start_date
        self.e_date = end_date
        self.c_date = check_date
        if check_date == '':
            yesterday = datetime.now() + timedelta(days = -1)
            self.c_date = yesterday.strftime("%Y%m%d")
            print self.c_date
        self.start_urls = [self.get_query_url(self.c_date, self.page_cnt)]
        super(NaverQuickSpider, self).__init__(*args, **kwargs)

    '''
    Get the query url
    '''
    def get_query_url(self, check_date, page):
        #qs = {'query': keyword}

        return 'http://news.naver.com/main/list.nhn?sid1=001&mid=sec&mode=LSD&listType=paper' \
                + '&date=' + check_date \
                + '&page=' + str(page) \
                


    '''
    Starting point
    Retrieve the news link from the list of search results.
    Args:
     response - the response object pertaining to the search results page
    '''
    def parse(self, response):
        # next page end condition
        next_button = response.xpath('//td[@class="content"]//div[@class="paging"]/a[@class="next"]')
        #if self.page_cnt >10:
        if len(next_button) == 0 and self.page_cnt >= int(response.xpath('//td[@class="content"]//div[@class="paging"]/a/text()').extract()[-1]):
            print "!!!!!!!!!!!!!get max page" + str(self.page_cnt)
            return
        # determine whether to go ahead with parse or not
        news_list= response.xpath('//td[@class="content"]//div[@id="main_content"]//li')

        print 'Page %s' % self.page_cnt
        print 'news_size is :' + str(len(news_list))
        cnt = 0
        for news_article in news_list:
            try:
                
                # news agency
                agency = news_article.xpath('.//span[@class="writing"]/text()').extract()[0]
                
                if agency not in [u'경향신문',u'중앙일보',u'한겨레',u'동아일보',u'조선일보']:
                    continue
                # naver news link
                news_url = news_article.xpath('.//a/@href').extract()[0]
                
                #naver news title
                news_title = news_article.xpath('.//a/text()').extract()[0]
                
                #naver news date
                news_date = news_article.xpath('.//span[@class="date"]/text()').extract()[0]
                
                #naver news paper
                news_position = news_article.xpath('.//span[@class="paper"]/text()').extract()[0]
                
                # parse news link to get aid and oid
                parsed_news_url = urlparse(news_url)
                
                #host_part = parsed_news_url[1]
                query_string = parse_qs(parsed_news_url[4])
                
                # populate article item
                #if query_string['oid'][0] != '32':
                #    continue                
                article = NaverArticleItem()
                article['aid'] = query_string['aid'][0]
                article['oid'] = query_string['oid'][0]
                article['agency'] = agency
                article['date'] = news_date
                article['title'] = news_title
                article['position'] = news_position
                
                req = scrapy.Request(news_url, callback = self.parse_news, dont_filter = self.dont_filter)

                article['referer'] = response.url
                req.meta['article'] = article
                #print article
                yield req
                
                cnt += 1
            except Exception, e:
                print 'ERROR!!!!!!!!!!!!!  URL :'
                print traceback.print_exc(file = sys.stdout)
                #pass

        print 'read %s articles' % cnt
        
        self.page_cnt += 1
        next_page_url = self.get_query_url(self.c_date, self.page_cnt)
        yield scrapy.Request(next_page_url, callback = self.parse, dont_filter = self.dont_filter)


    '''
    Retrieve the comment count link from a given news article.
    Args:
     response - the response object pertaining to the news article page
    '''
    def parse_news(self, response):

        # populate the rest of the article
        article = response.meta['article']
        article['url'] = response.url

        title = ''
        date = ''

        parsed_response_url = urlparse(response.url)
        host_part = parsed_response_url[1]

        if host_part == 'entertain.naver.com':
            title = response.css('p.end_tit').xpath('.//text()').extract()[0]
            date = response.css('div.article_info > span.author > em').xpath('.//text()').extract()[0]
            date = time.strftime('%Y-%m-%d %H:%M:00', self.parse_date(date))
            contents = ' '.join(response.css('div#articeBody').xpath('.//text()').extract()).strip()
        elif host_part == 'sports.news.naver.com':
            title = response.css('div.articlehead > h4').xpath('.//text()').extract()[0]
            date = response.css('div.info_article > span.time').xpath('.//text()').extract()[0]
            contents = ' '.join(response.css('div.article > div').xpath('.//text()').extract()).strip()

        else:
            title = response.css('div.article_info > h3').xpath('.//text()').extract()[0]
            date = response.css('div.article_info > div.sponsor > span.t11').xpath('.//text()').extract()[0]
            contents = ' '.join(response.css('div#articleBodyContents').xpath('.//text()').extract()).strip()

        article['title'] = title
        article['contents'] = contents
        article['date'] = date

        # this is the hidden 'comment count' api used by naver
        comment_check_url = 'http://m.news.naver.com/api/comment/count.json'

        comment_count_data = {
            'gno' : 'news' + article['oid'] + ',' + article['aid']
        }

        req = scrapy.FormRequest(comment_check_url, formdata = comment_count_data, callback = self.parse_comment_count, dont_filter = self.dont_filter)
        req.meta['article'] = article

        return req

    '''
    Retrieve comment count for a given news article.
    Args:
     response - the response object pertaining to the json response of the comment count api call
    '''
    def parse_comment_count(self, response):
        json_response = json.loads(response.body)
        comment_count = int(json_response['message']['result']['count'])
        self.update_count('comments', comment_count)

        yield response.meta['article']
        self.update_count('ayield', 1)

        if comment_count > 0:

            # this is the hidden 'comment list' api used by naver
            comment_url = 'http://m.news.naver.com/api/comment/list.json'

            comment_data = {
                'gno' : 'news' + response.meta['article']['oid'] + ',' + response.meta['article']['aid'],
                'page': '1',
                'sort': 'newest',
                'pageSize': str(comment_count),
                'serviceId' : 'news'
            }

            req = scrapy.FormRequest(comment_url, formdata = comment_data, callback = self.parse_comments, dont_filter = self.dont_filter)
            req.meta['article'] = response.meta['article']
            yield req

    '''
    Retrieve the list of comments for a given news article
    Args:
     response - the response object pertaining to the json response of the comment list api call
    '''
    def parse_comments(self, response):
        json_response = json.loads(response.body)
        for comment in json_response['message']['result']['commentReplies']:

            new_time = self.parse_date(comment['sRegDate'])

            comment_item = NaverCommentItem()
            comment_item['date'] = time.strftime('%Y-%m-%d %H:%M:00', new_time)
            comment_item['aid'] = response.meta['article']['aid']
            comment_item['username'] = comment['userNickname']
            comment_item['like_count'] = comment['goodCount']
            comment_item['dislike_count'] = comment['badCount']
            comment_item['contents'] = comment['content']

            yield comment_item

    '''
    Parse a date string in the form of '2015.07.10 오후 2:39' and return a time object
    Args:
     orig_date_str - the origina string to parse
    Returns:
     python time object
    '''
    def parse_date(self, orig_date_str):

        # date is in the form of '2015.07.10 오후 2:39'
        # change to python time object
        regex_res = re.match(u'^(\d{4}\.\d{2}\.\d{2})(.*?)(\d{1,2}:\d{2})$', orig_date_str, re.M|re.S)

        am_or_pm = regex_res.group(2).strip()
        if am_or_pm == u'\uc624\uc804':
            am_or_pm = 'AM'
        elif am_or_pm == u'\uc624\ud6c4':
            am_or_pm = 'PM'

        new_time_str = '%s %s %s' % (
            regex_res.group(1),
            regex_res.group(3),
            am_or_pm
        )

        new_time = time.strptime(new_time_str, '%Y.%m.%d %I:%M %p')
        return new_time

    '''
    Debug method - update deleted column
    '''
    def update_deleted(self, url):

        try:
            conn = MySQLdb.connect(
                    host = 'localhost',
                    user = 'mers',
                    passwd = 'Kb459CKS7nQLsHbD',
                    charset = 'utf8'
                    )
            cur = conn.cursor()
            conn.select_db('mers')

            sql = "update articles set deleted = 'Y' where url = '%s'" % (url)
            cur.execute(sql)
            conn.commit()
            cur.close()
            conn.close()
        except MySQLdb.Error, e:
            print 'MySQL error %d: %s' % (e.args[0], e.args[1])


    '''
    Debug method - update the given count
    '''

    def update_count(self, tpe, cnt):
        try:
            conn = MySQLdb.connect(
                    host = 'localhost',
                    user = 'mers',
                    passwd = 'Kb459CKS7nQLsHbD',
                    charset = 'utf8'
                    )
            cur = conn.cursor()
            conn.select_db('mers')

            sql = 'update counts set v = v + %s where k = "%s"' % (cnt, tpe)
            cur.execute(sql)
            conn.commit()
            cur.close()
            conn.close()
        except MySQLdb.Error, e:
            print 'MySQL error %d: %s' % (e.args[0], e.args[1])

    '''
    Debug method - insert the given url
    '''

    def update_url(self, url):
        try:
            conn = MySQLdb.connect(
                    host = 'localhost',
                    user = 'mers',
                    passwd = 'Kb459CKS7nQLsHbD',
                    charset = 'utf8'
                    )
            cur = conn.cursor()
            conn.select_db('mers')

            sql = 'insert into urls set url = "%s"' % url
            cur.execute(sql)
            conn.commit()
            cur.close()
            conn.close()
        except MySQLdb.Error, e:
            print 'MySQL error %d: %s' % (e.args[0], e.args[1])

