# -*- coding: UTF-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import  os, hashlib, redis, time, MySQLdb
from tornado.web import MissingArgumentError, authenticated
from handlers import BaseHandler

PRE_UPLOADER = "file_uploader_"
MAX_UPLOADER_SIZE = 4 # 最大上传块大小， 单位： MB
MAX_UPLOADER_WAITING = 60 # 单块上传等待时间

class Uploader(BaseHandler):
    def f_name(self, name): 
        uid = self.get_current_user() 
        md5 = hashlib.md5()
        md5.update(uid + name)
        return md5.hexdigest()

    def uploading(self, f_name):
        name = self.f_name(f_name) 
        r = redis.Redis() 
        k_exceptions = PRE_UPLOADER + "expection:%s" % name
        k_processings = PRE_UPLOADER + "inprocessing:%s" % name 
        rt = {"code": 0, "msg":  u"等会再来吧", "time":10}
        exceptions = r.smembers(k_exceptions)
        processings = r.hgetall(k_processings)
        exceptions = sorted(exceptions)
        for e2, processing in processings.items():
            if time.time() - float(processing) > MAX_UPLOADER_WAITING:  
                 # 上传时间过期了
                 start, end = map(int, e2.split("-"))
                 rt = {"code":0, "expection": [start, end] }
                 r.hset(k_processings, e2, time.time()) 
                 r.expire(k_processings, 10 * 10) #  
                 break  
        else: #上传中的文件块没有过期的
            for e1 in exceptions:
                if e1 in processings.keys():
                    continue # e1 在上传中 
                else:# 没e1有在上传中
                    start, end = map(int, e1.split("-"))
                    if end-start > MAX_UPLOADER_SIZE:
                        idx = exceptions.index(e1)
                        exceptions = exceptions[:idx] + exceptions[idx+1:] 
                        e1 = str(start)+"-"+str(start+MAX_UPLOADER_SIZE)
                        e2 = str(start+MAX_UPLOADER_SIZE)+"-"+str(end)
                        exceptions.extend([e1, e2])
                        exceptions = set(exceptions)
                        r.delete(k_exceptions)
                        r.sadd(k_exceptions, *exceptions)    
                        r.expire(k_exceptions, 2 * 60 * 60)
                        start, end = start, start + MAX_UPLOADER_SIZE
                    r.hset(k_processings, e1, time.time()) 
                    # 如果每个子块都没有上传成功， 还不停的请求任务， 是有bug, redis 不会过期了
                    r.expire(k_exceptions, 2 * 60 * 60)
                    rt = {"code":0, "expection": [start, end] }
                    break
            else: # redis 中没有期望上传块了, 1: 上传完了; 2: key 过期了 
                size = 1000000
                start, end = 0, 0 + MAX_UPLOADER_SIZE 
                r.sadd(k_exceptions, str(start)+"-"+str(MAX_UPLOADER_SIZE), str(end)+"-"+str(size))
                r.hset(k_processings, str(start)+"-"+str(end), time.time())
                r.expire(k_processings, 10 * 100) #  
                r.expire(k_exceptions, 2 * 60 * 60)
                rt = {"code":0, "expection": [start, end] }
        self.j_write(rt)   

    #@authenticated
    def get(self, f_name):
        return self.uploading(f_name)

    def put(self, f_name):
        title = self.get_argument("title")
        size = self.get_argument("size")
        try:
            name = self.f_name(f_name) 
            owner_id = self.get_current_user() 
            int(size)
        except :
             raise MissingArgumentError("参数丢失")
        sql = "insert ignore into files(`_name`, `title`, `owner_id`, `size`) "\
              "values ('%s', '%s', '%s', %s)"
        sql = sql % (name, MySQLdb.escape_string(title), owner_id, size)
        conn = MySQLdb.connect(host='localhost',user='root',passwd='',db='upload_files')
        conn.set_character_set('utf8')  
        cur = conn.cursor()   
        id = cur.execute(sql)
        conn.commit()
        if not id:
            rt = {"code":0, "msg":"不允许重复上传"}
        rt = {"code":1, "msg":"开始上传"} 
        self.j_write(rt)

    #@authenticated
    def post(self, f_name):
        name = self.f_name(f_name) 
        r = redis.Redis() 
        k_exceptions = PRE_UPLOADER + "expection:%s" % name
        k_processings = PRE_UPLOADER + "inprocessing:%s" % name 
        start = self.get_argument("start", "")
        end = self.get_argument("end", "")
        if start and end:
            block = "-".join([start, end])
            r = redis.Redis()
            if not r.sismember(k_exceptions, block):
                return self.uploading(f_name) 
            d_path = os.path.join("static", name) 
            if not os.path.exists(d_path):
                os.mkdir(d_path)
            f_path = os.path.join(d_path, block)
            if not os.path.exists(f_path):
                f = open(f_path, "w+")
                if f:  
                    data = self.get_argument("data", "")
                    if data:
                      #  if len(data) != int(end)-int(start) * 1024 * 2018: # 大小不够
                      #      return self.uploading(f_name)
                        f.write(data)
                        f.close()
                        r = redis.Redis()
                        r.hdel(k_processings, block)
                        r.srem(k_exceptions, block)
                        # clearup it from storeage # brocast 
        self.uploading(f_name)
