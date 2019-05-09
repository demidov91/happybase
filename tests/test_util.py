"""
HappyBase utility tests.
"""

from codecs import decode, encode
from unittest import mock

from nose.tools import assert_equal, assert_less, assert_raises

import happybase.util as util


def test_camel_case_to_pep8():
    def check(lower_cc, upper_cc, correct):

        x1 = util.camel_case_to_pep8(lower_cc)
        x2 = util.camel_case_to_pep8(upper_cc)
        assert_equal(correct, x1)
        assert_equal(correct, x2)

        y1 = util.pep8_to_camel_case(x1, True)
        y2 = util.pep8_to_camel_case(x2, False)
        assert_equal(upper_cc, y1)
        assert_equal(lower_cc, y2)

    examples = [('foo', 'Foo', 'foo'),
                ('fooBar', 'FooBar', 'foo_bar'),
                ('fooBarBaz', 'FooBarBaz', 'foo_bar_baz'),
                ('fOO', 'FOO', 'f_o_o')]

    for a, b, c in examples:
        yield check, a, b, c


def test_bytes_increment():
    def check(s_hex, expected):
        s = decode(s_hex, 'hex')
        v = util.bytes_increment(s)
        v_hex = encode(v, 'hex')
        assert_equal(expected, v_hex)
        assert_less(s, v)

    test_values = [
        (b'00', b'01'),
        (b'01', b'02'),
        (b'fe', b'ff'),
        (b'1234', b'1235'),
        (b'12fe', b'12ff'),
        (b'12ff', b'13'),
        (b'424242ff', b'424243'),
        (b'4242ffff', b'4243'),
    ]

    assert util.bytes_increment(b'\xff\xff\xff') is None

    for s, expected in test_values:
        yield check, s, expected


def test_retryable__once():
    real_method = mock.Mock(side_effect=[ValueError, 42])
    f = util.retryable(real_method, ValueError)

    assert f(202, '03') == 42

    assert real_method.call_count == 2
    real_method.assert_called_with(202, '03')


def test_retryable__once_with_callback():
    callback_method = mock.Mock()
    real_method = mock.Mock(side_effect=[ValueError, 42])
    f = util.retryable(real_method, ValueError, callback=callback_method)

    assert f(202, '03') == 42

    assert real_method.call_count == 2
    callback_method.assert_called_once_with(202, '03')
    real_method.assert_called_with(202, '03')


def test_retryable__twice_error():
    real_method = mock.Mock(side_effect=[ValueError, ValueError, 42])
    f = util.retryable(real_method, ValueError)

    with assert_raises(ValueError):
        f(202, '03')

    assert real_method.call_count == 2
    real_method.assert_called_with(202, '03')


def test_retryable__once_but_different():
    real_method = mock.Mock(side_effect=[NotImplementedError, 42])
    f = util.retryable(real_method, ValueError)

    with assert_raises(NotImplementedError):
        f(202, '03')

    real_method.assert_called_once_with(202, '03')


def test_retryable__twice_error_still_working():
    callback_method = mock.Mock()
    real_method = mock.Mock(side_effect=[ValueError, ValueError, 42])
    f = util.retryable(real_method, ValueError, retry_count=2, callback=callback_method)

    assert f(202, '03') == 42

    assert real_method.call_count == 3
    assert callback_method.call_count == 2
    real_method.assert_called_with(202, '03')


def test_retryable__different_errors_still_working():
    real_method = mock.Mock(side_effect=[NotImplementedError, ValueError, 42])
    f = util.retryable(real_method, (ValueError, NotImplementedError), retry_count=2)

    assert f(202, '03') == 42

    assert real_method.call_count == 3
    real_method.assert_called_with(202, '03')