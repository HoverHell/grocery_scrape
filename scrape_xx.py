#!/usr/bin/env python3
"""
...
"""
# pylint: disable=cell-var-from-loop,fixme


import sys
import re
import urllib
import threading
import logging
import json

import bs4
import requests
from requests.packages.urllib3.util import Retry  # pylint: disable=import-error


# with open('.proxy_auth.txt') as fobj:
#     PROXY_AUTH = fobj.read().strip()
# PROXY_AUTH = 'Basic U29...'


class WorkerBase:

    _max_errors = 1000

    retry_conf = Retry(
        total=5, backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504, 521],
        method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE', 'POST']),
    )

    def __init__(self):
        self._all_errors = []  # TODO: deque (limited)
        self.mgmt_lock = threading.Lock()
        session = requests.Session()
        retry_conf = self.retry_conf
        for prefix in ('http://', 'https://'):
            session.mount(
                prefix,
                requests.adapters.HTTPAdapter(
                    max_retries=retry_conf,
                    pool_connections=30, pool_maxsize=30,
                ))
        session.trust_env = False
        self.reqr = session
        self.categories = None
        self.items_data = {}  # url -> item data
        self.failures = []  # (kind, url)

    def get(self, *args, allow_redirects=True, **kwargs):

        rfs = kwargs.pop('rfs', True)

        headers = dict(kwargs.pop('headers', None) or {})
        # headers['Proxy-Authorization'] = PROXY_AUTH

        resp = self.reqr.get(
            *args,
            allow_redirects=allow_redirects,
            **kwargs)

        if rfs == '200':
            if resp.status_code != 200:
                raise Exception(
                    "Unexpected status code",
                    dict(resp=resp, status_code=resp.status_code))
        elif rfs:
            resp.raise_for_status()

        return resp

    def bs(self, resp):
        return bs4.BeautifulSoup(resp.content, 'html5lib')

    def try_(self, func, excs=(AttributeError, TypeError, ValueError), default=None):
        try:
            return func()
        except excs:
            exc_info = sys.exc_info()
            with self.mgmt_lock:
                self._all_errors.append(exc_info)
                if len(self._all_errors) > self._max_errors:
                    self._all_errors.pop(0)
            return default

    @staticmethod
    def map_require(func, iterable):
        # TODO?: ThreadPool / multiprocessing
        for item in iterable:
            func(item)

    def map_(self, func, iterable):
        # TODO?: ThreadPool / multiprocessing
        for item in iterable:
            self.try_(lambda: func(item))

    @staticmethod
    def el_text(el, default=None):
        if el is None:
            return default
        if isinstance(el, bs4.element.NavigableString):
            result = str(el)
        else:
            result = el.text
        # Turn the non-breakable spaces into the normal ones.
        result = result.replace('\xa0', ' ')
        return result

    def main(self):
        logging.basicConfig(level=logging.DEBUG)
        return self.main_i()

    def main_i(self):
        raise NotImplementedError


class WorkerUtk(WorkerBase):

    url_cats = 'https://www.utkonos.ru/cache/catalogue/megamenu/site/2/type/guest.html?_=1537439034420'
    url_cat_main = 'https://www.utkonos.ru/cat/{cat_id}'
    url_cat_page = 'https://www.utkonos.ru/cat/{cat_id}/page/{page_num}'

    def main_i(self):
        cats = self.get_cat_data()
        self.categories = cats
        try:
            self.map_(self.process_category, cats)
        finally:
            with open('utk_categories.json', 'w') as fobj:
                json.dump(self.categories, fobj)
            with open('utk_items.json', 'w') as fobj:
                json.dump(self.items_data, fobj)

    def get_cat_data(self):
        cat_resp = self.get(self.url_cats)
        cat_bs = self.bs(cat_resp)
        # self._debug(cat_s[:1000])

        cats = cat_bs.select('a.module_catalogue_megamenu-item')
        cats = list(
            dict(
                cat_id=cat.get('data-cat_id'),
                cat_parent_id=cat.get('data-parent_id'),
                cat_level=cat.get('data-level_id'),
                cat_href=cat.get('href'),
                cat_name=self.el_text(cat),
            )
            for cat in cats)
        # self._debug(cats[-1])
        return cats

    def get_max_page(self, bs):
        """
        Another method of getting the pages count (from the page footer text).
        Not currently used.
        """
        max_page = None
        stuff = bs.select_one('.el_paginate > .signature')
        if stuff:
            stuff = re.search(r'^Страница: [0-9]+ из ([0-9]+)$', self.el_text(stuff))
        if stuff:
            stuff = stuff.group(1)
        if stuff and stuff.isdigit():
            max_page = int(max_page)
        return max_page

    def process_category(self, cat):
        # max_page = self.get_max_page(...)
        for page in range(1, 9000):
            page_res = self.process_cat_page(cat=cat, page=page)
            # A bit tricky to parallelize because of this:
            if page_res and page_res.get('status') == 'redirected':
                break

    def process_cat_page(self, cat, page):
        if page == 1:
            url = self.url_cat_main.format(cat_id=cat['cat_id'])
        else:
            url = self.url_cat_page.format(cat_id=cat['cat_id'], page_num=page)

        page_resp = self.get(url, allow_redirects=False)
        if page_resp.status_code in (301, 302):  # pages over limit redirect to non-paged `url2`
            return dict(status='redirected')
        page_bs = self.bs(page_resp)

        items_special = page_bs.select('.goods_view_timetobuy > .goods_view_timetobuy-view')
        items_main = page_bs.select('.goods_view_box > .goods_view-item')
        items = list(items_special) + list(items_main)

        self.map_(lambda item_bs: self.process_item(item_bs, page_resp=page_resp), items)
        return {}

    def process_item(self, item_bs, page_resp):
        item_uri = item_bs.select_one('a.goods_caption').get('href')
        item_url = urllib.parse.urljoin(page_resp.request.url, item_uri)

        # A bit of simple caching.
        # Particularly necessary because upper categories contain most/all of the subitems.
        item_data = self.items_data.get(item_url)
        if item_data:
            return item_data

        item_resp = self.get(item_url)
        item_bs = self.bs(item_resp)

        base_url = item_resp.request.url
        item_data = dict(url=base_url)

        pic_bs = item_bs.select_one('.goods_view_item-pic')
        pic_variants = pic_bs.select(
            '.goods_view_item-variant_area'
            ' > a.goods_view_item-variant_item')
        item_data['pictures'] = list(
            urllib.parse.urljoin(base_url, el.get('data-pic-high'))
            for el in pic_variants)

        crumbs_bs = item_bs.select_one('.module_bread_crumbs')
        item_data['crumbs'] = list(
            dict(
                title=self.el_text(el),
                href=urllib.parse.urljoin(base_url, el.get('href', '')),
            )
            for el in crumbs_bs.select('.module_bread_crumbs-item > a'))

        preamble_bs = item_bs.select_one('.goods_view_item-preamble')
        # 'Артикул: ...'
        item_data['etc_preamble_original'] = self.try_(lambda: self.el_text(
            preamble_bs.select_one('.goods_view_item-preamble_original')))
        item_data['etc_rating'] = self.try_(lambda: int(
            preamble_bs.select_one('.goods_view_item-preamble_rating')
            .select_one('span.selected').get('data-ratingpos')))
        item_data['etc_rating_numvotes'] = self.try_(lambda: int(
            self.el_text(preamble_bs.select_one('.number_votes_text'))
            .replace(')', '').replace('(', '')))

        action_bs = item_bs.select_one('.goods_view_item-action')
        # NOTE: can also be grabbed from the last cru
        item_data['title'] = self.try_(lambda: self.el_text(
            action_bs.select_one('.goods_view_item-action_header')))
        item_data['title'] = item_data.get('title') or self.try_(lambda: self.el_text(
            crumbs_bs.select('.module_bread_crumbs-item')[-1]))

        item_data['etc_variants_something'] = self.try_(lambda: self.el_text(
            action_bs.select_one('.goods_variants_property-module')))

        prices_el = action_bs.select_one('.goods_price')
        item_data['etc_price_check'] = prices_el.get('data-static-now-price')
        for price_el in prices_el.select('.goods_price-item.current'):
            # NOTE: `.goods_price-item::after { content: '\20BD';` (“₽”)
            # suggests it is always in RUB.
            kind = price_el.get('data-weight')
            kind_to_key = {
                '/шт': 'price_per_piece',
                '/кг': 'price_per_kg',
                # grams?.. Multiply the price?..
            }
            price_key = kind_to_key.get(kind) or 'price_per_something'
            value = self.try_(lambda: float(
                self.el_text(price_el).replace(',', '.').replace(' ', '')))
            item_data[price_key] = value

        item_data['etc_max_purchase'] = self.try_(lambda: self.el_text(
            action_bs.select_one('.goods_view_item-limit_max')))

        item_data['etc_descriptions'] = list(
            self.el_text(el)
            for el in item_bs.select_one('#goods_view_item-tabs=description > div').children)

        props = item_bs.select('.goods_view_item-property_item')
        item_data['props'] = {
            self.try_(lambda: self.el_text(
                el.select_one('.goods_view_item-property_title'))):
            self.try_(lambda: self.el_text(
                el.select_one('.goods_view_item-property_value')))
            for el in props}
        item_data['etc_props_links'] = {
            self.try_(lambda: self.el_text(
                el.select_one('.goods_view_item-property_title'))):
            self.try_(lambda: el.select_one('.goods_view_item-property_value > a').get('href'))
            for el in props}

        self.items_data[item_url] = item_data
        return item_data


if __name__ == '__main__':
    try:
        worker
    except NameError:
        worker = WorkerUtk()
    worker.main()
