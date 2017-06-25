from datetime import datetime, timedelta
from dao_elastic import DaoElastic
from multiprocessing import Pool
from os.path import exists
from pymongo.errors import DuplicateKeyError
from requests import get
from textract import process


_NEWSLETTER_URLS = [{'url': 'https://gallery.mailchimp.com/cd23226ada1699a77000eb60b/files/6f01d319-21d5-4e25-ad77-d3543a3b7911/Disinformation_Review_09.02.2017_eng.pdf',
                     'date': datetime(year=2017, month=2, day=9)}]
_NEWSLETTER_PATH = 'https://eeas.europa.eu/sites/eeas/files/'
_FILE_PREFIX = 'disinformation_review_'
_FILE_SUFFIX = '_eng.pdf'
_LINK_FAKE_SUFFIX = 'http://bit.ly/'
_FILTER_DOMAINS = ['youtube.com', 'un.org', 'europa.eu', 'nato.int',
                   'state.gov', 'securitycouncilreport.org', 'defense.gov',
                   'stopfake.org', 'theguardian.com', 'amnesty.org',
                   'justice.gov', 'telegraph.co.uk', 'euobserver.com',
                   'martenscentre.eu']


def _is_filtered(domain):
    for word in _FILTER_DOMAINS:
        if domain.rfind(word) >= 0:
            return True
    return False


def _get_domain(full_url):
    start = full_url.find('://') + len('://')
    end = full_url.find('/', start)
    assert start > 0
    assert end > start
    return full_url[start:end]


def _get_full_url(short_url):
    response = get(short_url, allow_redirects=False)
    assert response.status_code in [301, 302], \
        'Error: link [%s] returned [%d]' % (short_url, response.status_code)
    html = response.text
    start = html.find('<a href="') + len('<a href="')
    end = html.find('">moved here')
    assert start > 0
    assert end > start
    return html[start:end]


def _merge_values(fake_links, date):
    dao = DaoElastic()
    for short_url in fake_links:
        if not dao.exists_short_url(short_url=short_url):
            full_url = _get_full_url(short_url)
            if not dao.exists_full_url(full_url=full_url):
                domain = _get_domain(full_url)
                skip = _is_filtered(domain)
                try:
                    dao.save_new_link(short_url=short_url, full_url=full_url,
                                      domain=domain, skip=skip,
                                      newsletter_date=date)
                except DuplicateKeyError:
                    print('Duplicated url [%s] from [%s], ignored.'
                          % (full_url, short_url))


def _get_fake_links(text):
    result = []
    for word in text.decode("utf-8").split():
        if word.startswith(_LINK_FAKE_SUFFIX):
            result.append(word)
    return result


def _try_scarpe_url(url):
    try:
        file_name = '../resources' + url[url.rfind('/'):].lower()
        if not exists(file_name):
            response = get(url=url, allow_redirects=False)
            assert response.status_code == 200
            print('Downloading file [%s].' % file_name)
            with open(file_name, 'wb') as file:
                for chunk in response:
                    file.write(chunk)
        else:
            print('Download skipped, file [%s] already downloaded.'
                  % file_name)
        fake_links = _get_fake_links(process(file_name))
        print('File [%s] contains links: [%r]' % (file_name, fake_links))
        return fake_links
    except AssertionError:
        pass


def _process_date(date):
    url = _NEWSLETTER_PATH + _FILE_PREFIX + date.strftime('%d.%m.%Y') + \
          _FILE_SUFFIX
    links = _try_scarpe_url(url=url)
    if links:
        _merge_values(links, date)


def _process_url(url, date):
    links = _try_scarpe_url(url=url)
    if links:
        _merge_values(links, date)


def scrape_from_path(days):
    end_date = datetime.utcnow()
    start_date = datetime.utcnow() - timedelta(days=days)
    date = start_date
    with Pool(4) as pool:
        while date <= end_date:
            pool.apply_async(_process_date, [date])
            date += timedelta(days=1)
        pool.close()
        pool.join()


def scrape_from_urls(urls):
    with Pool(4) as pool:
        for url in urls:
            pool.apply_async(_process_url, [url['url'], url['date']])
        pool.close()
        pool.join()


if __name__ == '__main__':
    scrape_from_path(10)
    scrape_from_urls(_NEWSLETTER_URLS)
