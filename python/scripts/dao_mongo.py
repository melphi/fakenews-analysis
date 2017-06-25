from datetime import datetime
from pymongo import MongoClient


class DaoMongo(object):
    def __init__(self):
        client = MongoClient('localhost', 27017)
        db = client.fakeko
        self._collection = db['news']
        self._collection.create_index('short_url', unique=True)
        self._collection.create_index('full_url', unique=True)

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
        self._collection.insert_one({'short_url': short_url,
                                     'full_url': full_url,
                                     'domain': domain,
                                     'skip': skip,
                                     'newsletter_date': date_str})

    def exists(self, *, short_url):
        assert short_url is not None and len(short_url) > 0
        return bool(self._collection.find_one({'short_url': short_url}))

    def save_error(self, *, short_url, error_message, error_class):
        """
        Arguments:
            short_url (str)
            error_message (str)
            error_class (str)
        """
        assert short_url is not None and len(short_url) > 0
        assert error_message is not None and len(error_message) > 0
        assert error_class is not None and len(error_class) > 0
        self._collection.update_one({'short_url': short_url},
                                    {'$set': {
                                        'error_message': error_message,
                                        'error_class': error_class},
                                    '$unset': {'text_analysed': ''}})

    def save_text_analysis(self, *, short_url, text_original, text_en, authors,
                           language, sentiment_score, sentiment_magnitude,
                           entities, extractor, translator):
        """
        Arguments:
            short_url (str)
            text_original (str)
            text_en (str)
            authors (str) Comma separated list of authors.
            language (str)
            sentiment_score (double)
            sentiment_magnitude (double)
            entities (list of object)
            extractor (str) The article extraction service used.
            translator (str) The translation service used.
        """
        assert short_url is not None and len(short_url) > 0
        assert text_original is not None and len(text_original) > 0
        assert text_en is not None and len(text_en) > 0
        assert language is not None and len(language) > 0
        assert entities, 'Missing entities'
        entities_dict = [{'name': entity.name,
                          'type': entity.entity_type,
                          'salience': entity.salience,
                          'wikipedia_url': entity.wikipedia_url}
                         for entity in entities]
        self._collection.update_one({'short_url': short_url},
                                    {'$set': {
                                        'text_original': text_original,
                                        'text_en': text_en,
                                        'translator': translator,
                                        'authors': authors,
                                        'language': language,
                                        'sentiment_score': sentiment_score,
                                        'sentiment_magnitude':
                                            sentiment_magnitude,
                                        'entities': entities_dict,
                                        'extractor': extractor,
                                        'text_analysed': True},
                                    '$unset': {'error_message': '',
                                               'error_class': ''}})

    def find_all(self):
        return self._collection.find()

    def find_for_text_analysis(self):
        return self._collection.find({'skip': {'$ne': True},
                                      'text_analysed': {'$ne': True}})

    def update_newsletter_date(self, short_url, date):
        date_str = date.strftime('%Y-%m-%d')
        self._collection.update_one({'short_url': short_url},
                                    {'$set': {'newsletter_date': date_str}})
