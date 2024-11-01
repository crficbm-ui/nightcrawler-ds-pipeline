import re
from typing import Optional
from urllib.parse import quote_plus
import json
import logging

import requests
from requests import Response

from nightcrawler.helpers.decorators import retry_on_requests_exception
from nightcrawler.helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


@retry_on_requests_exception(delay=300)
def make_request(
    request: requests.Request, token_to_mask: str = None
) -> requests.Response:
    request = request.prepare()
    masked_request_string = convert_request_to_string(request, token_to_mask)
    logger(f"request: {masked_request_string}")
    session = requests.Session()
    response = session.send(request)
    response.raise_for_status()
    return response


def convert_request_to_string(
    req: requests.models.PreparedRequest, token_to_mask: Optional[str] = None
) -> str:
    result = f"method: {req.method}, url: {req.url}"
    if req.body:
        result += ", body: " + req.body
    if not token_to_mask:
        return result
    return _mask_token_in_string(result, quote_plus(token_to_mask))


def convert_response_to_string(
    response: Response, token_to_mask: Optional[str] = None
) -> str:
    try:
        # Attempt to get json formatted data from response and turn it to CloudWatch-friendly format
        result = json.dumps(response.json())
    except json.decoder.JSONDecodeError:
        result = response.text

    if not token_to_mask:
        return result
    return _mask_token_in_string(result, token_to_mask)


def _mask_token_in_string(string_to_mask: str, token: str) -> str:
    return re.sub(re.escape(token), f"{re.escape(token[:5])}*****", string_to_mask)
