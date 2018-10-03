#!/usr/bin/env python3
"""
...
"""
# pylint: disable=cell-var-from-loop,fixme

import os
import sys
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

    def write_item(self, data, filename=None):
        if filename is None:
            filename = self.items_file
        data_s = json.dumps(data) + '\n'
        with self.mgmt_lock:
            with open(filename, 'a', 1) as fobj:
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
