#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-07 14:17:15
# @Last Modified by:   edward
# @Last Modified time: 2015-11-09 16:35:24
import os
from tornado.web import (
    RequestHandler, Application, url, HTTPError,authenticated)
from tornado.ioloop import IOLoop
from tornado.options import define
import tornado
from db import DB
from datetime import datetime as DateTime, timedelta
import time, random
import json
import hashlib

# mydml
CLIENT_SECRETS = {'ios':'9e304d4e8df1b74cfa009913198428ab'}

def getuniquestring():
    randstr = str(time.time()).replace(".", "")[4:]
    while len(randstr) < 8:
        randstr += "0"
    randstr += str(random.randint(10, 99))
    return randstr

def produce_token(code, client_id, client_secret):
    _el = [code, client_id, client_secret]
    _access = 'access'.join(_el)
    _access_toke = hashlib.md5(_access).hexdigest()
    _refresh = 'refresh'.join(_el)
    _refresh_token = hashlib.md5(_refresh).hexdigest()
    return _access_toke, _refresh_token

class Handler(RequestHandler):
    def get_argument_into(self, *args, **kwargs):
        into = kwargs.pop('into', None)
        r = self.get_argument(*args,**kwargs)
        if into is not None:
            r = into(r)
        return r

class RegisterHandler(Handler):
    def post(self):
        username = self.get_argument('username')
        password = self.get_argument('password')
        dml = DB.dml()
        # dml.table('user').insert(username=username, passwd=password).commit()
        dml.table('user').where(uid=2).update(passwd='dddddd').commit()
        # dml.table('user').where(uid=9).delete().commit()
        # captcha


class OtherHtmlHandler(RequestHandler):

    def get_current_user(self):
        return self.get_secure_cookie('user')

    def get(self):
        if self.current_user:
            self.write("hello, I am %s." % self.current_user)
        else:
            self.redirect("/")

class LoginHandler(RequestHandler):

    def get(self):
        self.write("<form method='post'><input type='text' name='user'>\
            <input type='submit'></form>")

    def post(self):
        # 使用cookie来存储用户信息
        self.set_secure_cookie('user', self.get_argument('user', None), expires_days=None)
        self.write("登陆成功")
        
        

class HomeHandler(RequestHandler):


    # 自定义过滤器
    def test_string(self, msg):
        return 'test string %s' % msg

    def get(self):
        self.ui['test_function'] = self.test_string
        self.write("Hello,world")


class MainHanlder(RequestHandler):
    def get(self):
        items = ["Item 1","Item 2","Item 3",]
        self.render('tmp.html', title='My Title', items=items)

class TestData(Handler):
    def get(self):
        start = self.get_argument_into('startpos', 0, into=int)
        stop = start + self.get_argument_into('count', 10, into=int)
        results = DB.dql().table('code_table').limit(0,2).queryset.all()
        for r in results:
            r['code_produce_time'] = DateTime.strftime(r['code_produce_time'], '%Y-%m-%d')
        self.write({'testdata': results})
class AccessCodeHandler(Handler):
    def get(self):
        client_id = self.get_argument_into('client_id', None)
        response = {'result': 0}
        if client_id in CLIENT_SECRETS:
            randstr = getuniquestring()
            DB.dml().table('code_table').insert({"code_access_code": randstr})
            response = {'result':1 , "access_code": randstr}
        jsonstr = json.dumps(response)
        self.set_header('Content-Type','application/json')
        self.write(jsonstr)

def IsTokenExists(code):
    codeObj = DB.dql().table('code_table', 'c').\
            inner_join('token_table', on="c.code_id=t.token_codeid", alias="t").\
            where(code_access_code=code).queryone()
    return False if codeObj is None else True

def IsValidCode(code):
    #  1 code is valid
    #  0 code is used
    # -1 code is invalid
    # -2 code is expired
    access_code = DB.dql().table('code_table').where(code_access_code=code).queryone()
    if access_code is None:
        return -1
    else:
        # Any access-code would be expired in 30 mins.
        expires_limit = timedelta(minutes=30)
        now_time = DateTime.now()
        produce_time = access_code['code_produce_time']
        if (now_time - produce_time) <= expires_limit:
            return (not IsTokenExists(code) and 1 or 0)
        else:
            return -2

def IsValidClient(client_id, client_secret):
    return (CLIENT_SECRETS.get(client_id) == client_secret)

class AccessTokenHanler(Handler):
    def get(self):
        client_id = self.get_argument_into('client_id', None)
        client_secret = self.get_argument_into('client_secret', None)
        code = self.get_argument_into('code', None)
        validateCode = IsValidCode(code)
        if validateCode == True and IsValidClient(client_id, client_secret):
            codeObj = DB.dql().table('code_table').where(code_access_code=code).queryone()
            code_id = codeObj and codeObj['code_id']
            access_token, refresh_token = produce_token(code, client_id, client_secret)
            DB.dml().table('token_table').insert({
                                    "token_codeid": code_id,
                                    'token_access_token': access_token,
                                    'token_refresh_token': refresh_token})
            response = {"result":1, "token": {
                                    "access_token": access_token,
                                    "refresh_token": refresh_token,
                                    "expires_in":3600, }}
        else:
            response = {'result': validateCode  }
        jsonstr = json.dumps(response)
        self.set_header('Content-Type','application/json')
        self.write(jsonstr)
# tornado资源配置
settings = {
    'template_path': os.path.join(os.path.dirname(__file__), 'templates'),
    'static_path': os.path.join(os.path.dirname(__file__),'static'),
    'login_url': '/login',
    'autoreload': True,
    'debug': True,
    'cookie_secret': 'abcdefg',
}

Application([
    (r'/?', HomeHandler),
    (r'/login/?', LoginHandler),
    (r'/other/?', OtherHtmlHandler),
    (r'/main/?', MainHanlder),
    (r'/data/?', TestData),
    (r'/register/?', RegisterHandler),
    (r'/access/code/?', AccessCodeHandler),
    (r'/access/token/?', AccessTokenHanler)
    ], **settings).listen(8888)

IOLoop.current().start()