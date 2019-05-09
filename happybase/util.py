"""
HappyBase utility module.

These functions are not part of the public API.
"""
import logging
import re

import six
from functools import partial, wraps
from six.moves import range

CAPITALS = re.compile('([A-Z])')
logger = logging.getLogger(__name__)


try:
    # Python 2.7 and up
    from collections import OrderedDict
except ImportError:
    try:
        # External package for Python 2.6
        from ordereddict import OrderedDict
    except ImportError as exc:
        # Stub to throw errors at run-time (not import time)
        def OrderedDict(*args, **kwargs):
            raise RuntimeError(
                "No OrderedDict implementation available; please "
                "install the 'ordereddict' Package from PyPI.")


def camel_case_to_pep8(name):
    """Convert a camel cased name to PEP8 style."""
    converted = CAPITALS.sub(lambda m: '_' + m.groups()[0].lower(), name)
    if converted[0] == '_':
        return converted[1:]
    else:
        return converted


def pep8_to_camel_case(name, initial=False):
    """Convert a PEP8 style name to camel case."""
    chunks = name.split('_')
    converted = [s[0].upper() + s[1:].lower() for s in chunks]
    if initial:
        return ''.join(converted)
    else:
        return chunks[0].lower() + ''.join(converted[1:])


def thrift_attrs(obj_or_cls):
    """Obtain Thrift data type attribute names for an instance or class."""
    return [v[1] for v in obj_or_cls.thrift_spec.values()]


def thrift_type_to_dict(obj):
    """Convert a Thrift data type to a regular dictionary."""
    return dict((camel_case_to_pep8(attr), getattr(obj, attr))
                for attr in thrift_attrs(obj))


def ensure_bytes(str_or_bytes, binary_type=six.binary_type,
                 text_type=six.text_type):
    """Convert text into bytes, and leaves bytes as-is."""
    if isinstance(str_or_bytes, binary_type):
        return str_or_bytes
    if isinstance(str_or_bytes, text_type):
        return str_or_bytes.encode('utf-8')
    raise TypeError(
        "input must be a text or byte string, got {}"
        .format(type(str_or_bytes).__name__))


def bytes_increment(b):
    """Increment and truncate a byte string (for sorting purposes)

    This functions returns the shortest string that sorts after the given
    string when compared using regular string comparison semantics.

    This function increments the last byte that is smaller than ``0xFF``, and
    drops everything after it. If the string only contains ``0xFF`` bytes,
    `None` is returned.
    """
    assert isinstance(b, six.binary_type)
    b = bytearray(b)  # Used subset of its API is the same on Python 2 and 3.
    for i in range(len(b) - 1, -1, -1):
        if b[i] != 0xff:
            b[i] += 1
            return bytes(b[:i+1])
    return None


def retryable(
        func=None,
        expected_exception=(),
        *,
        retry_count: int=1,
        callback=None,
):
    if func is None:
        return partial(
            retryable,
            expected_exception=expected_exception,
            retry_count=retry_count,
            callback=callback
        )

    @wraps(func)
    def wrapper(*args, **kwargs):
        i = 0

        prev_exceptions = []

        while True:
            try:
                return func(*args, **kwargs)
            except expected_exception as e:
                prev_exceptions.append(e)
                if i >= retry_count:
                    logger.error(
                        'Got too many exceptions: %s\nRaising the last one.',
                        prev_exceptions
                    )
                    raise e

                logger.info(
                    'Got expected exception %s while running %s. %s attempts left.',
                    type(e).__name__,
                    func.__name__ if hasattr(func, '__name__') else func,
                    retry_count - i
                )

                if callback is not None:
                    callback(*args, **kwargs)

                i += 1

    return wrapper


def retryable_generator(
        func=None,
        expected_exception=(),
        *,
        retry_count: int=1,
        callback=None,
):
    if func is None:
        return partial(
            retryable,
            expected_exception=expected_exception,
            retry_count=retry_count,
            callback=callback
        )

    @wraps(func)
    def wrapper(*args, **kwargs):
        i = 0

        prev_exceptions = []

        while True:
            try:
                object_to_return = func(*args, **kwargs)

                try:
                    yield next(object_to_return)
                except StopIteration:
                    return
                except GeneratorExit:
                    pass
            except expected_exception as e:
                prev_exceptions.append(e)
                if i >= retry_count:
                    logger.error(
                        'Got too many exceptions: %s\nRaising the last one.',
                        prev_exceptions
                    )
                    raise e

                logger.info(
                    'Got expected exception %s while running %s. %s attempts left.',
                    type(e).__name__,
                    func.__name__ if hasattr(func, '__name__') else func,
                    retry_count - i
                )

                if callback is not None:
                    callback(*args, **kwargs)

                i += 1
            else:
                yield from object_to_return
                return

    return wrapper
