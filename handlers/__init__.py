#! -*- coding:utf8 -*-
import json, traceback
from tornado.web import RequestHandler, MissingArgumentError

class Locker(object):
    def __enter__(self, key, pwd, length=10):
        pass
    def __exit__(self, *args, **kwargs):
        pass  

class BaseHandler(RequestHandler):
    def get_current_user(self):
        return "123"
        return self.get_secure_cookie("uid", None)
 
    def set_current_user(self, uid):
        return self.set_secure_cookie("uid", uid)

    def j_write(self, val):
       self.set_header("Content-Type", "application/json") 
       rt = ''  
       if isinstance(val, str):
          rt = val
       else:
          rt = json.dumps(val)
       self.write(rt)

    def log_exception(self, typ, value, tb):
        err = str(traceback.format_exc())
        if isinstance(value, MissingArgumentError):
            rt = {'code': 400, "info" : "参数不足"}
            self.set_status(400)
        else:
            rt = {'code' : 500, 'info' : 'server error'}
            self.set_status(500)
        raise
        #err = str(traceback.format_exc())
        self.set_status(400)
        self.j_write(rt)
        self.finish() 
 

from files import Uploader, Downloader
from user import Login
__all__ = ("BaseHandler", "Uploader", "Downloader"  "user.Login")


