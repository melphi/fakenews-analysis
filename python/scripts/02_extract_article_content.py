from bs4 import BeautifulSoup
from dao_elastic import DaoElastic
from google.cloud import language, translate
from google.cloud.exceptions import GoogleCloudError
from json import dumps
from multiprocessing import Pool
from requests import get
from urllib.parse import quote_plus
from xml.etree import ElementTree

_DIFFBOT_API_KEY = '<yourapikey>'
_DIFFBOT_API_URL = 'https://api.diffbot.com/v3/article'
_EMBEDLY_API_KEY = '<yourapikey>'
_EMBEDLY_API_URL = 'https://api.embedly.com/1/extract'
_EMBEDLY_LANGUAGES = {'English': 'en'}
# To get this token curl --header 'Ocp-Apim-Subscription-Key: <yourapikey>' --data "" 'https://api.cognitive.microsoft.com/sts/v1.0/issueToken'
_AZURE_COGNITIVE_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzY29wZSI6Imh0dHBzOi8vYXBpLm1pY3Jvc29mdHRyYW5zbGF0b3IuY29tLyIsInN1YnNjcmlwdGlvbi1pZCI6IjBiYjVhOGUzYTgzYzRjMGRhZWIyMzA3ZmFhNDM5ZTVlIiwicHJvZHVjdC1pZCI6IlRleHRUcmFuc2xhdG9yLkYwIiwiY29nbml0aXZlLXNlcnZpY2VzLWVuZHBvaW50IjoiaHR0cHM6Ly9hcGkuY29nbml0aXZlLm1pY3Jvc29mdC5jb20vaW50ZXJuYWwvdjEuMC8iLCJhenVyZS1yZXNvdXJjZS1pZCI6Ii9zdWJzY3JpcHRpb25zL2QwYmMwYTYzLWNmZjktNGNiZi04OWRjLWYzOTAzZWMzN2RjNy9yZXNvdXJjZUdyb3Vwcy91bmZhY3QvcHJvdmlkZXJzL01pY3Jvc29mdC5Db2duaXRpdmVTZXJ2aWNlcy9hY2NvdW50cy91bmZhY3QiLCJpc3MiOiJ1cm46bXMuY29nbml0aXZlc2VydmljZXMiLCJhdWQiOiJ1cm46bXMubWljcm9zb2Z0dHJhbnNsYXRvciIsImV4cCI6MTQ4NzY5NDczOX0.17hhcXacTdzDDoTXviKIbYnYXBHaHA6KPZIkwXGJ9NQ'


dao = DaoElastic()


def _get_article_content_diffbot(full_url):
    url = '%s?token=%s&url=%s' % (
        _DIFFBOT_API_URL, _DIFFBOT_API_KEY, quote_plus(full_url.strip()))
    response = get(url)
    assert response.status_code == 200, \
        'Invalid response [%d] for url [%s].' % \
        (response.status_code, full_url)
    content_json = response.json()
    assert 'objects' in content_json, \
        'Diffbot bad response: [%s]' % dumps(content_json)
    assert len(content_json['objects']) == 1, \
        'Expected [1] object but [%d] found.' % len(content_json['objects'])
    json = content_json['objects'][0]
    authors = json['author'] if 'author' in json else None
    return {'text': json['text'],
            'authors': authors,
            'language': json['humanLanguage'],
            'extractor': 'diffbot'}


def _get_article_content_embedly(full_url):
    url = '%s?key=%s&url=%s' % (
        _EMBEDLY_API_URL, _EMBEDLY_API_KEY, quote_plus(full_url.strip()))
    response = get(url)
    assert response.status_code == 200, \
        'Invalid response [%d] for url [%s].' % \
        (response.status_code, full_url)
    json = response.json()
    content = json['content']
    assert content, 'Not content extracted for [%s].' % full_url
    soup = BeautifulSoup(content, 'html.parser')
    authors = ','.join(json['authors'] if 'authors' in json else None)
    return {'text': soup.get_text(),
            'authors': authors,
            'language': _EMBEDLY_LANGUAGES[json['language']],
            'extractor': 'embedly'}


def _internal_translate_microsoft(text_part):
    """
    Args:
        text_part (str) The text to be translated
    Returns:
        (str) The text translation
    """
    params = {
        'text': text_part.encode('utf8'),
        'to': 'en',
        'contentType': 'text/plain'}
    headers = {
        'Authorization': 'Bearer %s' % _AZURE_COGNITIVE_TOKEN,
        'Accept': 'application/xml'}
    url = 'https://api.microsofttranslator.com/v2/http.svc/Translate'
    response = get(url, params=params, headers=headers)
    if response.status_code == 414:
        raise ValueError('Text size [%d] exceeds quota.' % len(text_part))
    assert response.status_code == 200, \
        'Invalid response status [%d].' % response.status_code
    return ElementTree.XML(response.text).text


def _get_translation_microsoft(text_content):
    result = ''
    if len(text_content) > 10000:
        for text in text_content.split('\n'):
            result = result + _internal_translate_microsoft(text) + '\n'
    else:
        try:
            result = _internal_translate_microsoft(text_content)
        except ValueError:
            for text in text_content.split('\n'):
                result = result + _internal_translate_microsoft(text) + '\n'
    return {'translator': 'microsoft', 'text_en': result}


def _get_translation_google(text_content):
    client = translate.Client()
    try:
        text_en = client.translate(
            text_content, target_language='en')['translatedText']
        return {'translator': 'google', 'text_en': text_en}
    except GoogleCloudError as e:
        if e.code == 400:
            raise ValueError('Malformed request.')
        elif e.code == 403:
            raise ValueError('Daily limit exceeded.')
        elif e.code == 413:
            raise ValueError('Text size [%d] exceeds quota.' %
                             len(text_content))
        else:
            raise e


# Change function body to use another article extractor.
def _get_article_content(full_url):
    return _get_article_content_diffbot(full_url)


# Change function body to use another translator.
def _get_translation(text_content):
    return _get_translation_microsoft(text_content)


def _process_text(news):
    """
    Args:
        news (dict)
    """

    assert news['id'], 'Missing news id for url [%s]' % news['short_url']
    assert 'skip' not in news or not news['skip'], \
        'News [%s] should not have [skip] True' % news['id']
    assert 'text_analysed' not in news or not news['text_analysed'], \
        'News [%s] should not have [text_analysed] True' % news['id']
    try:
        content = _get_article_content(news['full_url'])
        text_orig = content['text']
        if content['language'] != 'en':
            translation = _get_translation(text_orig)
        else:
            translation = {'translator': 'none', 'text_en': text_orig}
        document = language.Client().document_from_text(translation['text_en'])
        assert document is not None, 'Document object is none'
        annotated = document.annotate_text(include_syntax=False,
                                           include_entities=True,
                                           include_sentiment=True)
        assert annotated is not None, 'Annotated object is none'
        dao.save_text_analysis(news=news,
                               text_original=content['text'],
                               authors=content['authors'],
                               text_en=translation['text_en'],
                               translator=translation['translator'],
                               language=content['language'],
                               sentiment_score=
                               annotated.sentiment.score,
                               sentiment_magnitude=
                               annotated.sentiment.magnitude,
                               entities=annotated.entities,
                               extractor=content['extractor'])
        print('Url [%s] processed.' % news['short_url'])
    except Exception as e:
        print('Error while processing [%s]: [%s].' % (
            news['short_url'], str(e)))
        dao.save_error(news=news, error_message=str(e),
                       error_class=e.__class__.__name__)


def run():
    # for news in dao.find_for_text_analysis():
    #     _process_text(news)
    with Pool(4) as pool:
        for news in dao.find_for_text_analysis():
            pool.apply_async(_process_text, [news])
        pool.close()
        pool.join()


if __name__ == '__main__':
    run()
