#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-10 16:26:05
# @Last Modified by:   edward
# @Last Modified time: 2015-11-10 16:56:16
__metaclass__ = type

def token_formula(n):
    """
    n: 'str' or iterable contains 'str' objects
    """
    if hasattr(n, '__iter__'): 
        l = n
    else:
        l = [n]


# ==========
class Token:

    def __init__(self, *args):
        """
        args expects to collect 'str' object
        """
        self._init_token(*args)

    @property
    def value(self):
        return self.get_value()

    def get_value(self):
        return self._value

    def _init_token(self, *args):
        self._value = None
        if len(args) == 0:
            return
        elif len(args) == 1:

        else:
            pass


class AccessToken(Token):
    pass

class RefreshToken(Token):
    pass