#!/usr/bin/env python
# coding: utf8

import requests
from lxml import etree
from queue import Queue
from threading import Thread
from urllib.parse import urlparse


class Parser(Thread):
    '''解析器'''
    def __init__(self, name, result_queue):
        self.name = name            # 线程名字
        self.task = Queue()         # 带解析的 html 队列
        self.result = result_queue  # 已完成的结果队列
        self.setDaemon(True)        # 防止卡住主线程

    @staticmethod
    def url_filter(html):
        '''解析页面中的 URL'''
        # html 解析
        doc = etree.HTML(html)
        page_urls = set(doc.xpath('//a/@href'))

        # URL 格式化
        urls = set()
        for url in page_urls:
            parsed = urlparse(url)
            if parsed.netloc in ['', 'm.sohu.com']:
                scheme = parsed.scheme or 'http'
                query = parsed.query
                url = '%s://m.sohu.com/%s?%s' % (scheme, parsed.path ,query)
                urls.add(url)

        # 去除已访问过的 URL
        urls = urls.difference(done)

        return urls

    def run(self):
        '''执行线程'''
        print("%s run" % self.name)
        while True:
            html = self.task.get()
            urls = self.url_filter(html)
            for url in urls:
                self.result.put(url)


class Downloader(Thread):
    '''下载器'''
    def __init__(self, name, result_queue):
        self.name = name            # 线程名字
        self.task = Queue()         # 待下载的 url 队列
        self.result = result_queue  # 已完成的结果队列
        self.setDaemon(True)        # 防止卡住主线程

    @staticmethod
    def get(url):
        '''下载一个页面'''
        print('Getting: %s' % url)
        resp = requests.get(url)
        done.add(url)
        html = resp.text
        return html

    def run(self):
        '''执行线程'''
        print("%s run" % self.name)
        while True:
            url = self.task.get()
            html = self.get(url)
            self.result.put(html)


def main():
    '''调度器'''
    done = set()  # 已访问过的 URL
    html_queue = Queue()
    urls_queue = Queue()

    # 创建初始下载线程
    downloader_pool = []
    for i in range(1000):
        worker = Downloader("downloader-%s" % i, html_queue)
        downloader_pool.append(worker)
        worker.start()

    # 创建初始解析线程
    parser_pool = []
    for i in range(10):
        worker = Parser("parser-%s" % i, urls_queue)
        parser_pool.append(worker)
        worker.start()

    # 添加初始任务
    url = 'http://m.sohu.com'
    urls_queue.put(url)

    # 任务调度
    while True:
        while not urls_queue.empty():
            url = urls_queue.get()                              # 取出一个待下载的 URL
            downloader_pool.sort(key=lambda w: w.task.qsize())  # 按任务队列大小排序
            worker = downloader_pool[0]                         # 取出一个任务队列最小的下载线程
            worker.task.put(url)                                # 分派任务

        while not html_queue.empty():
            html = html_queue.get()                         # 取出一个待解析的 HTML
            parser_pool.sort(key=lambda w: w.task.qsize())  # 按任务队列大小排序
            worker = parser_pool[0]                         # 取出一个任务队列最小的下载线程
            worker.task.put(html)                           # 分派任务


if __name__ == '__main__':
    main()

