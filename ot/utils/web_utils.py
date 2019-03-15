from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from ot.logging import get_logger
from pprint import pprint

log = get_logger()


def simple_get(url):
    """
    Attempts to get the content at `url` by making an HTTP GET request.
    If the content-type of response is some kind of HTML/XML, return the
    text content, otherwise return None.
    """
    try:
        with closing(get(url, stream=True)) as resp:
            log.info('Connecting to {}'.format(url))
            if is_good_response(resp):
                log.info('Good response for url: {}'.format(url))
                return resp.content
            else:
                log.info('Bad response: {}'.format(resp))
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200
            and content_type is not None
            and content_type.find('html') > -1)


def log_error(e):
    """
    It is always a good idea to log errors.
    This function just prints them, but you can
    make it do anything.
    """
    print(e)


if __name__ == '__main__':
    pass
