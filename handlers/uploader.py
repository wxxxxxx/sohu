# -*- coding: UTF-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import  os, re, hashlib, redis, time, MySQLdb
from tornado.web import MissingArgumentError, authenticated
from handlers import BaseHandler

PRE_UPLOADER = "file_uploader_"
MAX_UPLOADER_SIZE = 3 * 1024 * 1024 # 最大上传块大小， 单位： b
MAX_UPLOADER_WAITING = 60 # 单块上传等待时间

class Uploader(BaseHandler):
    def _name(self, name): 
        uid = self.get_current_user() 
        md5 = hashlib.md5()
        md5.update(uid + name)
        return md5.hexdigest()

    def _exceptions(self, size, name):
        # 鲁棒性有点差， 不能修改 MAX_UPLOADER_SIZE 的大小,  
        exceptions = [[0, MAX_UPLOADER_SIZE], [MAX_UPLOADER_SIZE,size]]
        d_path = os.path.join("static",name)
        if not os.path.exists(d_path):
            return exceptions
        files = os.listdir(d_path)
        if not files:
            return exceptions
        exceptions = [[0,size]]
        r = re.compile("^\d+-\d+$")
        files = sorted(files)
        f = lambda x: map(int, x.split("-"))
        files = map(f, files)
        for l in files:
            for idx in xrange(len(exceptions)):
                e = exceptions[idx]
                if e in files:
                     exceptions = exceptions[:idx] + exceptions[idx+1:]
                     continue
                if e[0] <= l[0] and l[1] <= e[1]:
                    a, b = l[0]-e[0], e[1] - l[1]
                    new = []
                    if a > 0:
                        new.append([e[0], l[0]])
                    if b > 0:
                        new.append([l[1], e[1]])
                    if new:
                        exceptions = exceptions[:idx] + exceptions[idx+1:]
                        exceptions.extend(new)
                    break
        return sorted(exceptions)

    def __exceptions(self, size, name="c431d9c42836ac66cd26af0918cdffc2"):
        # 鲁棒性有点差， 不能修改 MAX_UPLOADER_SIZE 的大小,  
        exceptions = ["0/%s"%size]
        d_path = os.path.join("static",name)
        if not os.path.exists(d_path):
            return exceptions
        files = os.listdir(d_path)
        if not files:
            return exceptions
        exceptions = []
        r = re.compile("^\d+-\d+$")
        if files:
            files = sorted(files)
            pos = 0
            f = lambda x: x.split("-")
            l = map(f, files)
            max_file = sorted(files, cmp=cmp_file_name)[-1]
            start, end = map(int, max_file.split('-'))
            exceptions = [str(end)+"-"+str(size)]
            for i in xrange(0, end, MAX_UPLOADER_SIZE):
                 exceptions.append(str(i) + '-' + str(i+MAX_UPLOADER_SIZE))
            exceptions = list(set(exceptions) - set(files))
        return sorted(exceptions, cmp=cmp_file_name)

    def _next_uploading(self, f_name):
        name = self._name(f_name) 
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
                rt = {"code":2, "msg":"上传完成"}  
                conn = MySQLdb.connect(host='localhost',user='root',passwd='',db='upload_files')
                conn.set_character_set('utf8')  
                cur = conn.cursor()   
                sql = "select size, integerity from files where _name='%s' limit 1" % (name)
                cur.execute(sql)
                f = cur.fetchone()
                if not f:
                    rt = {"code":-1, "msg":"请先创建资源"}
                elif f[1] == 100:
                    rt = {"code":2, "msg":"上传完成了"}
                else:# 没上传完
                    size = f[0]     
                    exceptions = self._exceptions (size, name) 
                    integerity = 100 -  sum([e[1]-e[0] for e in exceptions]) * 100 / size
                    if not exceptions or 100 == integerity:
                        rt = {"code":2, "msg":"上传完成了"}
                        sql = 'update files set integerity=100 where _name="%s"' % name 
                        cur.execute(sql)
                        conn.commit() 
                    else:
                        exceptions = ["%s-%s"%(e[0],e[1]) for e in exceptions] 
                        r.sadd(k_exceptions, *exceptions)
                        r.hset(k_processings, exceptions[0], time.time())
                        r.expire(k_processings, 10 * 100) #  
                        r.expire(k_exceptions, 2 * 60 * 60)
                        rt = {"code":0, "expection": exceptions[0].split('-') }
        self.j_write(rt)   

    #@authenticated
    def get(self, f_name):
        self._next_uploading(f_name)

    def put(self, f_name):
        title = self.get_argument("title")
        size = self.get_argument("size")
        try:
            print f_name
            name = self._name(f_name) 
            owner_id = self.get_current_user() 
            int(size)
        except :
             raise MissingArgumentError("参数丢失")
        sql = "insert ignore into files(`_name`, `title`, `owner_id`, `size`) "\
              "values ('%s', '%s', '%s', %s)"
        sql = sql % (name, MySQLdb.escape_string(title), owner_id, size)
        print sql
        conn = MySQLdb.connect(host='localhost',user='root',passwd='',db='upload_files')
        conn.set_character_set('utf8')  
        cur = conn.cursor()   
        id = cur.execute(sql)
        conn.commit()
        os.mkdir(os.path.join("static", name))
        if not id:
            rt = {"code":0, "msg":"不允许重复上传"}
        rt = {"code":1, "msg":"开始上传"} 
        self.j_write(rt)

    #@authenticated
    def post(self, f_name):
        name = self._name(f_name) 
        r = redis.Redis() 
        k_exceptions = PRE_UPLOADER + "expection:%s" % name
        k_processings = PRE_UPLOADER + "inprocessing:%s" % name 
        start = self.get_argument("start", "")
        end = self.get_argument("end", "")
        if start and end:
            block = "-".join([start, end])
            r = redis.Redis()
            if not r.sismember(k_exceptions, block):
                return self._next_uploading(f_name) 
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
                      #      return self._next_uploading(f_name)
                        f.write(data)
                        f.close()
                        r = redis.Redis()
                        r.hdel(k_processings, block)
                        r.srem(k_exceptions, block)
                        # clearup it from storeage # brocast 
        self._next_uploading(f_name)
