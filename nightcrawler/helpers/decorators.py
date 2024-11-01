import functools
import requests
import logging
import time
from nightcrawler.helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class TemporaryError(Exception):
    """Temporary Connection Error. Retry"""

    pass


def retry_on_requests_exception(
    _func=None, *, number_of_retries: int = 3, delay: int = 0
):
    def retry_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(number_of_retries):
                logger.debug(
                    f"{func.__name__}:: Starting request, attempt_num: {i + 1}"
                )
                try:
                    result = func(*args, **kwargs)
                    logger.debug(f"{func.__name__}:: Request successfully completed")
                    return result
                except requests.exceptions.HTTPError as e:
                    if 400 <= e.response.status_code < 500:
                        logger.error(f"{func.__name__}:: Request failed -> abort")
                        raise
                    logger.error(f"{func.__name__}:: Request failed")
                    time.sleep(delay)
                except requests.exceptions.ConnectionError as e:
                    logger.warning(
                        f"{func.__name__}:: Connection error retry", exc_info=e
                    )
                    time.sleep(delay)
                except requests.exceptions.ReadTimeout as e:
                    logger.warning(
                        f"{func.__name__}:: ReadTimeout error retry", exc_info=e
                    )
                    time.sleep(delay)
                except TemporaryError as e:
                    logger.warning(f"{func.__name__}:: TemporaryError", exc_info=e)
                    time.sleep(delay)
            logger.error(f"{func.__name__}:: Request failed too many times -> abort")
            raise RuntimeError(f"{func.__name__}:: Request failed too many times")

        return wrapper

    if _func is None:
        return retry_decorator
    else:
        return retry_decorator(_func)


def log_start_and_end(_func=None, *, include_result: bool = False):
    def log_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"{func.__name__}: Started")
            result = func(*args, **kwargs)
            logger.info(
                f'{func.__name__}:: Finished {"Result:" + result if include_result else ""}'
            )
            return result

        return wrapper

    if _func is None:
        return log_decorator
    else:
        return log_decorator(_func)


def timeit(method):
    """
    Decorator that logs the time it took to run the method.

    Args:
        method (callable): The method to be timed.

    Returns:
        callable: The wrapped method with timing logic.
    """

    def timed(*args, **kwargs):
        start_time = time.perf_counter()
        result = method(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        class_name = args[0].__class__.__name__ if args else "UnknownClass"
        method_name = method.__name__

        if class_name == "Namespace" and method_name == "apply":
            # full pipeline run
            logger.info(f"Run full pipeline in {elapsed_time:.10f} seconds.")

        else:
            logger.debug(
                f"{class_name}{'.' + method_name if method_name != 'apply' else ''} took {elapsed_time:.10f} seconds to run."
            )

        return result

    return timed
