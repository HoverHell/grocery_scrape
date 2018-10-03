#!/usr/bin/env python3
"""
...
"""
# pylint: disable=cell-var-from-loop,fixme

import scrapy
from scraper_okd import WorkerOkey

class ScrapyOkd(scrapy.Spider):
    name = 'okd'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subworker = WorkerOkey()

    def start_requests(self):
        yield scrapy.Request(url=self.subworker.url_cats, callback=self.parse_cats)

    def parse_cats(self, response):
        base_el = response.css('#departmentsMenu').extract_first()
        cats = base_el.css('a.menuLink').extract()
        # yield response.follow(cat_url)
        raise Exception("TODO")


def main():
    from scrapy.crawler import CrawlerProcess
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:666.0) Gecko/20100101 Firefox/666.0',
    })
    process.crawl(ScrapyOkd)
    process.start()


if __name__ == '__main__':
    main()
