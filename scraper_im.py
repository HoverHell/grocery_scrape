#!/usr/bin/env python3
"""
...
"""
# pylint: disable=cell-var-from-loop,fixme

from scraper_base import (
    os, json, urllib,
    WorkerBase,
)


class WorkerImBase(WorkerBase):

    url_cats = None  # required
    cats_file = None  # required

    categories = None

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


if __name__ == '__main__':
    WorkerImCommon.run_all()
