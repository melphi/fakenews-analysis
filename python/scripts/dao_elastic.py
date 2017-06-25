from datetime import datetime
from json import load
from requests import get, head, post, put
from urllib.parse import quote_plus
from uuid import uuid4


class DaoElastic(object):
    _INDEX = 'unfact'
    _TYPE = 'news'
    _MAX_FETCH_SIZE = 300
    _BASE_HOST = 'http://127.0.0.1:9200'
    _MAPPING_FILE = '../resources/mapping.json'

    @staticmethod
    def _assert_response(response):
        assert response.status_code in [200, 201], \
            'Unexpected response [%d]: [%s]' % (
                response.status_code, response.json())
        return response

    def __init__(self, base_host=None):
        self._base_host = self._BASE_HOST if base_host is None else base_host
        self._base_index = self._base_host + '/' + self._INDEX
        self._base_url = self._base_index + '/' + self._TYPE
        self._init_schema()

    def _init_schema(self):
        """Sets up the index schema."""

        response = head('%s/_mapping/%s' % (self._base_index, self._TYPE))
        if response.status_code == 404:
            print('Index not found, creating mapping.')
            with open(self._MAPPING_FILE) as file:
                json = load(file)
                response = put(self._base_index, json=json)
                self._assert_response(response)
        elif response.status_code != 200:
            raise ValueError('Connection error to [%s]: [%r]' % (
                self._base_url, response.text))

    def save_new_link(
            self, *, short_url, full_url, domain, skip, newsletter_date):
        """
        Arguments:
            short_url (str)
            full_url (str)
            domain (str)
            skip (boolean)
            newsletter_date (datetime)
        """

        assert short_url is not None and len(short_url) > 0
        assert full_url is not None and len(full_url) > 0
        assert skip is not None
        assert newsletter_date is not None
        date_str = newsletter_date.strftime('%Y-%m-%d')
        news_id = str(uuid4()).replace('-', '')
        url = '%s/%s/_create' % (self._base_url, news_id)
        response = post(url, json={'id': news_id,
                                   'short_url': short_url,
                                   'full_url': full_url,
                                   'domain': domain,
                                   'skip': skip,
                                   'newsletter_date': date_str})
        self._assert_response(response)

    def exists_short_url(self, *, short_url):
        assert short_url is not None
        url = '%s/_search' % self._base_url
        query = {'query': {'constant_score': {'filter': {
            'term': {'short_url': short_url}}}}}
        response = get(url, json=query)
        self._assert_response(response)
        return response.json()['hits']['total'] > 0

    def exists_full_url(self, *, full_url):
        assert full_url is not None
        url = '%s/_search' % self._base_url
        query = {'query': {'constant_score': {'filter': {
            'term': {'full_url': full_url}}}}}
        response = get(url, json=query)
        self._assert_response(response)
        return response.json()['hits']['total'] > 0

    def save_text_analysis(self, news, text_original, authors, text_en,
                           translator, language, sentiment_score,
                           sentiment_magnitude, entities, extractor):
        """
        Arguments:
            news (dict): The complete news object
            text_original (str)
            authors (str): Comma separated list of authors
            text_en (str)
            translator (str)
            language (str)
            sentiment_score (str)
            sentiment_magnitude (str)
            entities (list of obj)
            extractor (str)
        """

        assert news['short_url'] is not None and len(news['short_url']) > 0
        assert news['id'] is not None and len(news['id']) > 0
        assert text_original is not None and len(text_original) > 0
        assert text_en is not None and len(text_en) > 0
        assert language is not None and len(language) > 0
        assert entities, 'Missing entities'
        entities_dict = [{'name': entity.name,
                          'type': entity.entity_type,
                          'salience': entity.salience,
                          'wikipedia_url': entity.wikipedia_url}
                         for entity in entities]
        news['text_original'] = text_original
        news['authors'] = authors
        news['text_en'] = text_en
        news['translator'] = translator
        news['language'] = language
        news['sentiment_score'] = sentiment_score
        news['sentiment_magnitude'] = sentiment_magnitude
        news['entities'] = entities_dict
        news['extractor'] = extractor
        url = '%s/%s' % (self._base_url, news['id'])
        response = put(url, json=news)
        self._assert_response(response)

    def save_error(self, *, news, error_message, error_class):
        """
        Arguments:
            news (dict): The complete news object
            error_message (str)
            error_class (str)
        """

        assert news['short_url'] is not None and len(news['short_url']) > 0
        assert news['id'] is not None and len(news['id']) > 0
        if 'text_analysed' in news:
            del news['text_analysed']
        news['error_message'] = error_message
        news['error_class'] = error_class
        url = '%s/%s' % (self._base_url, news['id'])
        response = put(url, json=news)
        self._assert_response(response)

    def import_news(self, news):
        news['id'] = str(news.pop('_id'))
        if 'tokens' in news:
            del news['tokens']
        if 'sentences' in news:
            del news['sentences']
        url = '%s/%s/_create' % (self._base_url, news['id'])
        response = put(url, json=news)
        if response.status_code == 409:
            print('Document [%s] was already present.', news['id'])
            return
        else:
            self._assert_response(response)

    def find_for_text_analysis(self, include_errors=False):
        must_not = [{'term': {'skip': 'true'}},
                    {'term': {'text_analysed': 'true'}}]
        if not include_errors:
            must_not.append({'exists': {'field': 'error_class'}})
        query = {'size': self._MAX_FETCH_SIZE,
                 'query':
                     {'constant_score': {'filter': {'bool': {
                         'must_not': must_not}}}}}
        response = get('%s/_search' % self._base_url, json=query)
        data = self._assert_response(response).json()
        if data['hits']['total'] > 0:
            for hit in data['hits']['hits']:
                yield hit['_source']
        else:
            return []
