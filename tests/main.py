#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: edward
# @Date:   2015-11-07 14:17:15
# @Last Modified by:   edward
# @Last Modified time: 2015-11-08 00:04:41
import os
from tornado.web import (
    RequestHandler, Application, url, HTTPError,authenticated)
from tornado.ioloop import IOLoop
from tornado.options import define
import tornado
from db import DB
# mydml


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
        results = mydql().table('student', 'o').limit(0,2).queryset.all()
        self.write({'testdata': results})
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
    ], **settings).listen(8888)

IOLoop.current().start()