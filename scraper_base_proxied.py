#!/usr/bin/env python3
"""
...
"""
# pylint: disable=cell-var-from-loop,fixme,abstract-method,arguments-differ

from scraper_base import (
    urllib,
    requests,
    WorkerBase,
    LOG,
)


def group(lst):
    res = {}
    for key, val in lst:
        try:
            group_list = res[key]
        except KeyError:
            res[key] = [val]
        else:
            group_list.append(val)
    return res


class WorkerBaseProxied(WorkerBase):

    proxies_iter = None
    proxy_arg = None
    proxy_retries = 3

    def _is_proxied_url(self, url, **kwargs):  # pylint: disable=unused-argument
        return False

    def _check_for_error_page(self, resp, **kwargs):
        return None

    def req(self, url, *args, tries=None, **kwargs):
        if not self._is_proxied_url(url):
            return super().req(url, *args, **kwargs)

        if tries is None:
            tries = self.proxy_retries

        if self.proxy_arg is None:
            self.proxies_iter = self.get_proxies()
            self.proxy_arg = next(self.proxies_iter)

        for retries_remain in reversed(range(tries)):
            kwargs['proxies'] = self.proxy_arg
            try:
                result = super().req(url, *args, **kwargs)
                self._check_for_error_page(result)
                return result
            except Exception:
                if not retries_remain:
                    raise
                self.proxy_arg = next(self.proxies_iter)
        raise Exception("Not even trying")

    def get_proxies(self, **kwargs):
        for item in self.get_proxies_fpl(**kwargs):
            yield item

    def _check_proxy(self, arg):
        try:
            resp = requests.get('https://example.com', proxies=arg, timeout=1)
            resp.raise_for_status()
        except Exception as exc:
            LOG.debug("Proxy %r error %r", arg, exc)
            return False
        return True

    def get_proxies_fpl(self):
        resp = self.req('https://www.free-proxy-list.net/')
        resp.raise_for_status()
        bs = self.bs(resp)
        rows = bs.select('table#proxylisttable > tbody > tr')
        if not rows:
            raise Exception("No proxy elements")
        LOG.info("Proxy elements count: %d", len(rows))
        for row in rows:
            try:
                cells = row.select('td')
                host, port, _, _, _, _, is_https, _ = cells[:8]  # pylint: disable=unbalanced-tuple-unpacking
                addr = '{proto}{host}:{port}'.format(
                    proto='https://' if self.el_text(is_https) == 'yes' else 'http://',
                    host=self.el_text(host),
                    port=self.el_text(port),
                )
                arg = dict(http=addr, https=addr)
                if self._check_proxy(arg):
                    yield arg
            except Exception as exc:
                LOG.warning("Proxylist error=%r, item=%r", exc, cells)

    def get_proxies_pp(self):
        for page in range(1, 9):
            url = 'https://premproxy.com/list/'
            if page != 1:
                url = '%s%02d.html' % (url, page)
            resp = self.req(url)
            bs = self.bs(resp)
            bs_form = bs.select_one('form[name=slctips]')
            req2_url = urllib.parse.urljoin(resp.url, bs_form.get('action') or {})
            req2_method = bs_form.get('method')
            req2_data = group(
                (elem.get('name') or '', elem.get('value') or '')
                for elem in bs_form.select('input'))

            resp2 = self.req(req2_url, method=req2_method, data=req2_data)
            bs2 = self.bs(resp2)
            addrs = list(
                self.el_text(elem)
                for elem in bs2.select('ul#ipportlist > li'))
            for addr in addrs:
                for proto in ('http://', 'https://'):
                    addr_full = '{}{}'.format(proto, addr)
                    arg = dict(http=addr_full, https=addr_full)
                    if self._check_proxy(arg):
                        yield arg

    def get_proxies_fpl2(self):
        try:
            import selenium
        except Exception:
            LOG.warning("get_proxies_fpl2: no `selenium`")
            return
        url = 'http://www.freeproxylists.net/'
        for page in range(1, 200):
            pass  # TODO
