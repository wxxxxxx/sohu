#! -*- coding:utf8 -*-

# -*- coding: UTF-8 -*-
from tornado.ioloop import IOLoop
import tornado.httpserver
import tornado.web
from tornado.options import options, define
import handlers
debug = True
define("debug", default=debug, help="debug mode")
import sys, os

reload(sys)
sys.setdefaultencoding("utf-8")
server_port = 9191
pid_num = 1

settings = {
    "cookie_secret": "61oETzKXQAGaY223kL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
    "gzip" : True,
    "debug" : options.debug,
     "static_path": os.path.join(os.path.dirname(__file__), "static"),
#    "xsrf_cookies" : True,
    "login_url" : 'login',
}


application = tornado.web.Application([
    (r'/file/upload/([0-9-a-zA_Z]+)', handlers.Uploader),
    (r'/file/download/([0-9-a-zA_Z]+)', handlers.Downloader),
#    (r'/login', handlers.Login),
],**settings)

def restart(signal):
    pass


if __name__ == "__main__":
    print 'development:  start listening ' + str(server_port)
    server = tornado.httpserver.HTTPServer(application, xheaders=True)
    server.bind(server_port)
    server.start(pid_num)
    tornado.ioloop.IOLoop.instance().start()
