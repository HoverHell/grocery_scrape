#!/usr/bin/env python3
"""
...
"""
# pylint: disable=cell-var-from-loop,fixme

import os
import sys
import re
import urllib
import threading
import datetime
import logging
import json
import traceback

import bs4
import requests
from requests.packages.urllib3.util import Retry  # pylint: disable=import-error


LOG = logging.getLogger(__name__)


def parse_url(url, base=None):
    url_full = None
    if base is not None:
        url_full = urllib.parse.urljoin(base, url)
    parts = urllib.parse.urlparse(url)
    paramses = urllib.parse.parse_qs(parts.query)
    params = dict(urllib.parse.parse_qsl(parts.query))
    return dict(
        url_full=url_full,
        url=url,

        scheme=parts.scheme,
        netloc=parts.netloc,
        path=parts.path,
        path_params=parts.params,
        fragment=parts.fragment,

        username=parts.username,
        password=parts.password,
        hostname=parts.hostname,
        port=parts.port,

        params=params,
        paramses=paramses,
    )


class WorkerBase:

    items_file = None  # required for `self.write_item`.

    _max_errors = 100

    retry_conf = Retry(
        total=25, backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504, 521],
        method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE', 'POST']),
    )

    force = False

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
        self.processed_items = set()
        self.failures = []  # (kind, url)

    @staticmethod
    def skip_none(dct):
        return {
            key: val for key, val in dct.items()
            if key is not None and val is not None}

    @staticmethod
    def read_jsl(filename, require=True):
        try:
            fobj = open(filename)
        except FileNotFoundError:
            if require:
                raise
            return
        for line in fobj:
            if not line:
                continue
            yield json.loads(line.strip())

    def collect_processed_items(self, key='url', filename=None):
        LOG.debug("Collecting previously processed addresses...")
        filename = filename or self.items_file
        for item in self.read_jsl(filename or self.items_file, require=False):
            # NOTE: if a particular field is particularly required,
            # this point can be used for debugging its gathering.
            self.processed_items.add(item.get(key))
        LOG.debug("Previously processed addresses: %d", len(self.processed_items))

    def req(self, *args, allow_redirects=True, method='get', default_headers=True, **kwargs):

        rfs = kwargs.pop('rfs', True)

        headers = dict(kwargs.pop('headers', None) or {})

        if default_headers:
            headers.update({
                'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:62.0) Gecko/20100101 Firefox/62.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            })

        resp = self.reqr.request(
            method,
            *args,
            allow_redirects=allow_redirects,
            headers=headers,
            **kwargs)

        if rfs == '200':
            if resp.status_code != 200:
                raise Exception(
                    "Unexpected status code",
                    dict(resp=resp, status_code=resp.status_code))
        elif rfs:
            resp.raise_for_status()

        return resp

    def get(self, *args, **kwargs):
        return self.req(*args, method='get', **kwargs)

    def bs(self, resp):
        # return bs4.BeautifulSoup(resp.content, 'html5lib')
        return bs4.BeautifulSoup(resp.text, 'html5lib')

    def try_(self, func, excs=(AttributeError, TypeError, ValueError), default=None, silent=False):
        try:
            return func()
        except excs as exc:
            exc_info = sys.exc_info()
            with self.mgmt_lock:
                self._all_errors.append(exc_info)
                if len(self._all_errors) > self._max_errors:
                    self._all_errors.pop(0)

            if not silent:
                _, _, etb = exc_info
                LOG.error(
                    '`try_`-wrapped error: %r; %s',
                    exc,
                    ''.join(traceback.format_tb(etb, 2)).replace('\n', ';'))
                if os.environ.get('IPDBG'):
                    traceback.print_exc()
                    import ipdb
                    _, _, sys.last_traceback = exc_info
                    ipdb.pm()

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
    def el_text(el, default=None, strip=True):
        if el is None:
            return default
        if isinstance(el, bs4.element.NavigableString):
            result = str(el)
        else:
            result = el.text
        # Turn the non-breakable spaces into the normal ones.
        result = result.replace('\xa0', ' ')
        if strip:
            result = result.strip()
        return result

    def write_item(self, data):
        data_s = json.dumps(data) + '\n'
        with self.mgmt_lock:
            with open(self.items_file, 'a', 1) as fobj:
                fobj.write(data_s)

    def write_data(self, filename, data):
        with open(filename, 'w') as fo:
            json.dump(data, fo)
            fo.write('\n')

    @staticmethod
    def now():
        return datetime.datetime.now().isoformat()

    def main(self):
        assert self.items_file
        logging.basicConfig(level=logging.DEBUG)
        return self.main_i()

    def main_i(self):
        raise NotImplementedError

    def process_item_url(self, item_url, **kwargs):
        if item_url in self.processed_items and not self.force:
            LOG.debug("Already processed: %s", item_url)
            return

        item_resp = self.get(item_url)
        base_url = item_resp.url
        item_data = dict(url=base_url, ts=self.now())
        item_bs = self.bs(item_resp)

        res_data = self.process_item_url_i(base_url, item_bs, item_resp=item_resp, **kwargs)
        item_data.update(res_data)

        self.write_item(item_data)
        with self.mgmt_lock:
            self.processed_items.add(item_url)

    def process_item_url_i(self, base_url, item_bs, **kwargs):
        raise NotImplementedError


class WorkerUtk(WorkerBase):

    items_file = 'utk_items.jsl'

    url_cats = 'https://www.utkonos.ru/cache/catalogue/megamenu/site/2/type/guest.html?_=1537439034420'
    url_cat_main = 'https://www.utkonos.ru/cat/{cat_id}'
    url_cat_page = 'https://www.utkonos.ru/cat/{cat_id}/page/{page_num}'

    def main_i(self):
        if not self.force:
            self.collect_processed_items()

        cats = self.get_cat_data()
        self.categories = cats
        self.write_data('utk_categories.json', cats)
        self.map_(self.process_category, cats)

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
        base_url = page_resp.url
        page_bs = self.bs(page_resp)

        items_special = page_bs.select('.goods_view_timetobuy > .goods_view_timetobuy-view')
        items_main = page_bs.select('.goods_view_box > .goods_view-item')
        items = list(items_special) + list(items_main)

        items_urls = list(
            (item_bs.select_one('a.goods_caption') or {}).get('href')
            for item_bs in items)
        items_urls = list(
            urllib.parse.urljoin(base_url, item_url)
            for item_url in items_urls if item_url)

        self.map_(self.process_item_url, items_urls)
        return {}

    def process_item_url_i(self, base_url, item_bs, **kwargs):
        item_data = {}
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

        return item_data


class WorkerImBase(WorkerBase):

    url_cats = None  # required
    cats_file = None  # required

    def main_i(self):
        assert self.url_cats
        assert self.cats_file

        if not self.force:
            self.collect_processed_items()

        cats = self.get_cat_data(self.url_cats)
        self.categories = cats
        self.write_data(self.cats_file, cats)
        self.map_(self.process_category, cats['urls'])

    def get_cat_data(self, url=None):
        if not self.force and os.path.exists(self.cats_file):
            return json.load(open(self.cats_file))

        cat_resp = self.get(url)
        base_url = cat_resp.url
        cat_bs = self.bs(cat_resp)

        cats = []
        urls = []
        for cat_el in cat_bs.select('a.taxon-title__link'):
            # A category linked in a category-listing page.
            cat_data = dict(
                url=urllib.parse.urljoin(base_url, cat_el.get('href')),
                title=self.el_text(cat_el),
            )
            # Linked page might be a category listing or a product listing.
            # Have to get the page; will request those pages twice as a result.
            subcats = self.get_cat_data(cat_data['url'])
            urls.extend(subcats['urls'])
            if subcats['cats']:
                cat_data['subcategories'] = subcats['cats']
            else:
                urls.append(cat_data['url'])
            cats.append(cat_data)

        return dict(cats=cats, urls=urls)

    def process_category(self, root_url):
        for page in range(1, 9000):
            page_resp = self.get('{}/page/{}'.format(root_url, page))
            base_url = page_resp.url
            page_bs = self.bs(page_resp)
            items_container_bs = page_bs.select_one('.products_with_filters_wrapper')
            emptiness_message = items_container_bs.select_one('.empty-filter-message')
            if emptiness_message is not None:  # supposedly, an empty page.
                break
            items_bses = items_container_bs.select('li.product')
            items_urls = list(
                (item_bs.select_one('a.product__link') or {}).get('href')
                for item_bs in items_bses)
            items_urls = list(
                urllib.parse.urljoin(base_url, item_url)
                for item_url in items_urls if item_url)
            self.map_(self.process_item_url, items_urls)

    def process_item_url_i(self, base_url, item_bs, **kwargs):
        item_data = {}

        item_bs_root = item_bs
        item_bs = item_bs_root.select_one('.product-popup')

        crumbs_bs = item_bs.select_one('.product-popup__breadcrumbs')
        crumb_els = self.try_(lambda: crumbs_bs.select('.product-popup__breadcrumbs-link'))
        item_data['crumbs'] = list(
            dict(
                url=urllib.parse.urljoin(base_url, crumb_el.get('href')),
                title=self.el_text(crumb_el),
            ) for crumb_el in crumb_els or ())

        img_el = item_bs.select_one('img.product-popup__img')
        if img_el:
            item_data['etc_image_preview'] = img_el.get('src')
            item_data['etc_image'] = img_el.get('data-zoom')

        item_data['title'] = self.el_text(item_bs.select_one('.product-popup__title'))

        item_data['amount_text'] = self.el_text(item_bs.select_one('.product-popup__volume'))

        item_data['price_text'] = self.el_text(item_bs.select_one('.product-popup__price'))

        desc_el = item_bs.select_one('.product-popup__description')
        if desc_el:
            item_data['description'] = list(
                el.decode()  # HTML almost-source.
                for el in desc_el.children)

        item_data['nutrition_title'] = self.el_text(item_bs.select_one('.nutrition .nutrition-title'))
        nutrition_props = item_bs.select('.nutrition .product-property')
        item_data['nutrition_properties'] = {
            self.el_text(elem.select_one('.product-property__name')):
            self.el_text(elem.select_one('.product-property__value'))
            for elem in nutrition_props}

        item_data['ingredients_text'] = self.el_text(item_bs.select_one('.ingredients__text'))

        other_props = item_bs.select('.other-properties .product-property')
        item_data['properties'] = self.skip_none({
            self.el_text(elem.select_one('.product-property__name')):
            self.el_text(elem.select_one('.product-property__value'))
            for elem in other_props})

        item_data['properties_links'] = self.skip_none({
            self.el_text(elem.select_one('.product-property__name')):
            self.try_(
                lambda: urllib.parse.urljoin(
                    base_url,
                    elem.select_one('.product-property__value a.product-link').get('href')),
                silent=True,
            )
            for elem in other_props})

        return item_data


class WorkerImCommon(WorkerImBase):

    known_names = ('metro', 'vkusvill', 'lenta', 'karusel')

    def __init__(self, name, **kwargs):
        '''
        :param name: see `known_names`.
        '''
        super().__init__(**kwargs)
        self.name = name

    @classmethod
    def run_all(cls):
        for name in cls.known_names:
            worker = cls(name)
            worker.main()

    url_cats = property(lambda self: 'https://instamart.ru/{}'.format(self.name))
    cats_file = property(lambda self: 'im_{}_categories.json'.format(self.name))
    items_file = property(lambda self: 'im_{}_items.jsl'.format(self.name))


class WorkerOkey(WorkerBase):

    url_cats = 'https://www.okeydostavka.ru/msk/catalog'
    cats_file = 'okd_categories.json'
    items_file = 'okd_items.jsl'

    def main_i(self):
        if not self.force:
            self.collect_processed_items()

        cats = self.get_cat_data()
        self.categories = cats
        self.write_data(self.cats_file, cats)
        self.map_(self.process_category, cats)

    def get_cat_data(self):
        if not self.force and os.path.exists(self.cats_file):
            return json.load(open(self.cats_file))

        cat_resp = self.get(self.url_cats)
        base_url = cat_resp.url
        cat_bs = self.bs(cat_resp)

        base_el = cat_bs.select_one('#departmentsMenu')
        cats = base_el.select('a.menuLink')

        def cat_parent(cat_el):
            cat_li_el = cat_el.parent
            # assert cat_li_el.name == 'li'
            upcat_ul_el = cat_li_el.parent
            # assert upcat_ul_el.name == 'ul'
            upcat_a_el = upcat_ul_el.find_previous_sibling('a')
            if not upcat_a_el:
                return None
            return upcat_a_el.get('id')

        def cat_data(cat_el):
            return dict(
                id=cat_el.get('id'),
                url=urllib.parse.urljoin(base_url, cat_el.get('href')),
                title=self.el_text(cat_el),
                parent_id=self.try_(lambda: cat_parent(cat_el)),
            )

        return list(cat_data(cat_el) for cat_el in cats)

    def process_category(self, cat):
        return self.process_category_url(cat['url'])

    def get_cat_page(self, store_id, catalog_id, cat_id, position=0, page_size=72, params=None):
        resp = self.req(
            'https://www.okeydostavka.ru/webapp/wcs/stores/servlet/ProductListingView',
            method='post',
            params=dict(
                params or {},
                # the category
                storeId=store_id,  # '10151',
                catalogId='12051',
                categoryId=cat_id,  # '30552',
                # notable
                resultsPerPage=page_size,
                # searchType='1000',
                # langId='-20',
                # sType='SimpleSearch',
                # custom_view='true',
                # ajaxStoreImageDir='/wcsstore/OKMarketSAS/',
                # disableProductCompare='true',
                # ddkey='ProductListingView_6_-1011_3074457345618259713',
                # # empties
                # resultCatEntryType='',
                # lm='',
                # filterTerm='',
                # advancedSearch='',
                # gridPosition='',
                # metaData='',
                # manufacturer='',
                # searchTerm='',
                # emsName='',
                # facet='',
                # filterFacet='',
            ),
            headers={
                'Accept': '*/*',
                'X-Requested-With': 'XMLHttpRequest',
            },
            data=dict(
                # page location; '0', '72', '144', '216', ...
                beginIndex=position,
                # same as beginIndex
                productBeginIndex=position,
                # notable
                pageSize=page_size,
                # hopefully unneeded
                # currentPage='Чай',
                # # unknowns
                # contentBeginIndex='0',
                # pageView='grid',
                # resultType='products',
                # storeId='10151',
                # ffcId='13151',
                # storeGroup='msk1',
                # catalogId='12051',
                # langId='-20',
                # userType='G',
                # userId='-1002',
                # currencySymbol='руб.',
                # businessChannel='-1',
                # mobihubVersion='011',
                # logonUrl='/webapp/wcs/stores/servlet/ReLogonFormView?catalogId=12051&myAcctMain=1&langId=-20&storeId=10151',
                # isB2B='false',
                # b2bMinCartTotal='',
                # maxOrderWeight='80',
                # iosAppId='1087812169',
                # imageDirectoryPath='/wcsstore/OKMarketSAS/',
                # isFfcMode='true',
                # objectId='_6_-1011_3074457345618259713',
                requesttype='ajax',
                # # empties
                # orderBy='',
                # facetId='',
                # orderByContent='',
                # searchTerm='',
                # facet='',
                # facetLimit='',
                # minPrice='',
                # maxPrice='',
                # logonId='',
                # userFirstName='',
                # userLastName='',
            ),
        )
        return resp

    def process_category_url(self, root_url):
        base_page_resp = self.get(root_url)
        base_url = base_page_resp.url
        base_page_bs = self.bs(base_page_resp)

        products = base_page_bs.select_one('.product_listing_container .product_name')
        if not products:
            LOG.debug("Likely a non-terminal category (no products): %s", root_url)
            return

        LOG.debug("Category page: %s", root_url)
        pages_params = None

        scripts = base_page_bs.select('script')
        sbn_scripts = list(
            script_el for script_el in scripts
            if '/webapp/wcs/stores/servlet/ProductListingView' in script_el.text)
        if sbn_scripts:
            uri_match = re.search("""['"]([^"']*/webapp/wcs/stores/servlet/ProductListingView[^"']+)['"]""", sbn_scripts[0].text)
            if uri_match:
                pages_uri = uri_match.group(1)
                pages_params = parse_url(pages_uri)['params']
        if not pages_params:
            # hlink = base_page_bs.select_one('a#contentLink_1_HeaderStoreLogo_Content')['href']
            # params = parse_url(hlink)['params']
            # store_id = params['storeId']
            # catalog_id = params['catalogId']
            # # See also:
            # # base_page_bs.select_one('a#advancedSearch')['href']
            # # ...
            search_inputs = base_page_bs.select('#searchBox > input')
            pages_params = {
                input_el['name']: input_el['value'] for input_el in search_inputs
                if input_el.get('value')}
            pages_params['categoryId'] = base_url.rsplit('-', 2)[-2]

        store_id = pages_params['storeId']
        catalog_id = pages_params['catalogId']
        cat_id = pages_params['categoryId']

        position = 0
        all_items = []
        for _ in range(1, 9000):
            page_resp = self.get_cat_page(
                store_id=store_id, catalog_id=catalog_id, cat_id=cat_id,
                position=position)
            page_bs = self.bs(page_resp)
            page_items = page_bs.select('.product_name > a')
            LOG.info("Page items: %r", len(page_items))
            if not page_items:
                break
            position += len(page_items)
            all_items.extend(page_items)

        all_items_urls = [urllib.parse.urljoin(base_url, item_el['href']) for item_el in all_items]
        self.map_(self.process_item_url, all_items_urls)

    def process_item_url_i(self, base_url, item_bs, **kwargs):
        item_data = {}

        item_base_bs = item_bs
        item_bs = item_base_bs.select_one('.product_page_content')

        crumbs_bs = item_bs.select_one('#widget_breadcrumb')
        crumbs = list(
            dict(
                url=urllib.parse.urljoin(base_url, elem.get('href') or ''),
                title=self.el_text(elem))
            for elem in crumbs_bs.select('a'))
        crumbs += list(
            dict(title=self.el_text(elem))
            for elem in crumbs_bs.select('li.current'))
        item_data['crumbs'] = crumbs

        info_bs = item_bs.select_one('.product-information')

        item_data['title'] = self.el_text(info_bs.select_one('.main_header'))

        price_bs = item_bs.select_one('.product_price')
        item_data['price_crossed'] = self.el_text(price_bs.select_one('.crossed'))
        item_data['price'] = self.el_text(price_bs.select_one('.price'))

        chars_el = info_bs.select_one('.product-characteristics')
        if self.el_text(chars_el):
            item_data['characteristics_html'] = chars_el.decode()

        def parse_prop_elem(prop_elem):
            name = None
            value = None
            for subelem in prop_elem.children:
                if isinstance(subelem, bs4.element.NavigableString):
                    continue
                elif (subelem.get('id') or '').startswith('descAttributeName_'):
                    name = self.el_text(subelem)
                elif (subelem.get('id') or '').startswith('descAttributeValue_'):
                    value = self.el_text(subelem)
            return name, value

        props_els = item_bs.select('.widget-list > li')
        props = {}
        for elem in props_els:
            name, value = self.try_(lambda: parse_prop_elem(elem)) or (None, None)
            if not name or value is None:
                continue
            name_base = name
            for idx in range(10):
                if name not in props:
                    break
                name = '{}_{}'.format(name_base, idx)

            props[name] = value

        item_data['props'] = props
        return item_data


if __name__ == '__main__':
    # WorkerUtk().main()
    # WorkerImCommon.run_all()
    WorkerOkey().main()
