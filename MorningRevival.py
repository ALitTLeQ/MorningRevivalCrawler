# coding=UTF-8
__author__ = 'ee830804'

import logging
from Queue import Queue  # PY2
from threading import Thread
from bs4 import BeautifulSoup
import re
import requests
from requests.exceptions import RequestException

NUM_THREADS = 8


def requests_get(*args, **kwargs):
	"""
	Retries if a RequestException is raised (could be a connection error or
	a timeout).
	"""
	logger = kwargs.pop('logger', None)
	try:
		return requests.get(*args, **kwargs)
	except RequestException as exc:
		if logger:
			logger.warning('Request failed (%s). Retrying ...', exc)
		return requests.get(*args, **kwargs)


class XuiteBlogCrawler(object):
	url_templates = 'http://blog.xuite.net/%(username)s/%(category)s'

	def __init__(self, username, category, log_level=logging.WARNING):
		# Logging
		self.set_logger(log_level, init=True)

		self.base_url = self.url_templates % {'username': username, 'category': category}

		self.logger.info(self.base_url)

	def set_logger(self, log_level, init=False):
		if init:
			self.logger = logging.getLogger('python-craiglist')
			self.handler = logging.StreamHandler()
			self.logger.addHandler(self.handler)
		self.logger.setLevel(log_level)
		self.handler.setLevel(log_level)

	def getList(self, page=1, title_filters=None):
		page_url = self.base_url + '?&p=%s'% page
		self.logger.info(page_url)

		response = requests_get(page_url, logger=self.logger)
		response.raise_for_status()
		soup = BeautifulSoup(response.content, 'html.parser')

		articles = set()
		for box in soup.findAll('h3', {'class': 'title'}):
			for a in box.findAll('a'):
				articleId = str(a['href']).split('/')[-1]
				if articleId.isdigit():
					self.logger.info(articleId)
					if title_filters == None or title_filters in a.text:
						articles.add(articleId)
						print a.text
		return articles


	def getArticle(self, articleId):
		article_url =self.base_url + '/%s'% articleId
		response = requests_get(article_url, logger=self.logger)
		response.raise_for_status()
		soup = BeautifulSoup(response.content, 'html.parser')

		result = {}
		title = soup.find('span', {'class': 'titlename'})
		if title:
			titleName = title.text

			contentList = []
			for box in soup.findAll('div', {'id': 'content_all'}):
				for span in box.findAll('span'):
					if(span.text):
						contentList.append(span.text)
			result = {
				'titleName' : titleName,
				'content' : contentList
			}

		return result


queue = Queue()
class MorningRevival():
	def __init__(self):
		self.xuiteBlog = XuiteBlogCrawler(username='ymch130', category='MorningRevival', log_level=logging.INFO)
		self.title_pattern = re.compile(u'晨興聖言-(.*)\(W(.*)-(.+)\)')
		self.all_articles = set()

	def getList(self, topN=1):

		for i in range(1, topN+1):
			articles = self.xuiteBlog.getList(page=i, title_filters= u"晨興聖言-")
			self.all_articles = self.all_articles | articles


		itemList = []
		for id in list(self.all_articles):
			queue.put(id)


		def worker():
			global queue
			while not queue.empty():
				id = queue.get()
				article = self.xuiteBlog.getArticle(id)
				if article:
					item = self.parseToItem(article)
					print item
					itemList.append(item)
					queue.task_done()


		threads = map(lambda i: Thread(target=worker), xrange(NUM_THREADS))  # Create 2 threads

		map(lambda th: th.start(), threads)  # Make all threads start their activities

		map(lambda th: th.join(), threads)   # block until all threads are terminated

		return itemList


	def parseToItem(self, article):
		m1 = self.title_pattern.match( article['titleName'])
		if m1:
			subject = m1.group(1)
			week =  m1.group(2)
			day = m1.group(3)
			title = article['titleName']
			content = article['content']

		result = {
			'subject' : subject,
			'week': week,
			'day': day,
			'title': title,
			'content': content
		}
		return result


if __name__ == '__main__':
	morningRevival = MorningRevival()
	itemList = morningRevival.getList(topN=10)
	print len(filter(lambda x: x['week'] == '42', itemList))
	print len(filter(lambda x: x['week'] == '41', itemList))
	print len(filter(lambda x: x['week'] == '40', itemList))
	print len(filter(lambda x: x['week'] == '39', itemList))