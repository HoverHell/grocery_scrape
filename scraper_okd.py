#!/usr/bin/env python3
"""
...
"""
# pylint: disable=cell-var-from-loop,fixme,arguments-differ

import re
from scraper_base import (
    os, json, urllib,
    bs4,
    LOG,
    parse_url,
)
from scraper_base_proxied import WorkerBaseProxied


class WorkerOkey(WorkerBaseProxied):

    url_host = 'https://www.okeydostavka.ru'
    url_cats = 'https://www.okeydostavka.ru/msk/catalog'
    cats_file = 'okd_categories.json'
    cat_items_file = 'okd_cat_items.jsl'
    items_file = 'okd_items.jsl'

    # ...

    def _is_proxied_url(self, url):
        return url.startswith(self.url_host)

    def _check_for_error_page(self, resp, bs=None):
        if bs is None:
            bs = self.bs(resp, check_for_error_page=False)
        title = self.el_text(bs.select_one('.title')) or ''
        if 'Bad IP' in title:
            message = self.el_text(bs.select_one('.message')) or ''
            raise Exception('Got ‘Bad IP’: {title!r} {message!r}.'.format(
                title=title, message=message))

    def bs(self, resp, check_for_error_page=True, **kwargs):
        bs = getattr(resp, '_bs_cached', None)
        if bs is None:
            bs = super().bs(resp, **kwargs)
            if check_for_error_page and self._is_proxied_url(resp.url):
                self._check_for_error_page(resp, bs)
        setattr(resp, '_bs_cached', bs)
        return bs

    # ...

    def main_i(self):
        if not self.force:
            self.collect_processed_items()

        cats = self.get_cat_data()
        self.categories = cats
        self.write_data(self.cats_file, cats)
        self.map_(self.process_category, cats)
        # ... passing the stuff through a file:
        cat_infos = self.read_jsl(self.cat_items_file)
        items_urls = (
            item_url
            for cat_info in cat_infos
            for item_url in cat_info['item_urls'])
        self.map_(self.process_item_url, items_urls)

    def collect_processed_items(self, *args, **kwargs):
        super().collect_processed_items(*args, **kwargs)
        for item in self.read_jsl(self.cat_items_file, require=False):
            self.processed_items.add(item.get('url'))
        LOG.debug(
            "Previously processed addresses (with category listings): %d",
            len(self.processed_items))

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
                catalogId=catalog_id,
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
        """ category base page url -> None; dumps the category items urls into a file """

        if root_url in self.processed_items and not self.force:
            LOG.debug("Already processed category listing: %s", root_url)
            return

        all_items_urls = self.process_category_url_i(root_url)
        cat_data = dict(url=root_url, item_urls=all_items_urls or [])
        self.write_item(cat_data, filename=self.cat_items_file)

    def process_category_url_i(self, root_url):
        """ category base page url -> category items urls """
        base_page_resp = self.get(root_url)
        base_url = base_page_resp.url
        base_page_bs = self.bs(base_page_resp)

        products = base_page_bs.select_one('.product_listing_container .product_name')
        if not products:
            subcats = base_page_bs.select('div.row.categories > div')
            if subcats:
                LOG.debug("A non-terminal category (no products, %d subcategories): %s", len(subcats), root_url)
                return None
            # else:
            with open('.okd_last_error_page.html', 'wb') as fo:
                fo.write(base_page_resp.content)
            raise Exception("Probably an error page at {}".format(root_url))

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
        return all_items_urls

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
    WorkerOkey().main()
