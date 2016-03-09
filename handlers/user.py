# -*- coding: UTF-8 -*-

import sys, os
from tornado.web import RequestHandler, MissingArgumentError

class Login(RequestHandler):
    def get(self):
        uid = self.get_argument("uid", "112233")
        self.set_secure_cookie("uid", uid)  
        self.write("hi there "+ uid)

    def post(self):
        self.get()

