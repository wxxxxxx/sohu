# -*- coding: UTF-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import conf
import  os, re, hashlib, redis, time, MySQLdb
from tornado.web import MissingArgumentError, authenticated
from handlers import BaseHandler

PRE_UPLOADER = "file_uploader_"
PRE_DOWNLOADER = "file_downloader_"
MAX_UPLOADER_SIZE = 3 #3 * 1024 * 1024 # 最大上传块大小， 单位： b
MAX_DOWNLOADER_SIZE = 30 #3 * 1024 * 1024 # 最大上传块大小， 单位： b
TIME_UPLOADER_WAITING = 200 # 单块上传等待时间
TIME_UPLOADER_EXPECTATIONS = 60 * 60 # 上传等待时间
TIME_DOWNLOADER = 60 * 60 # 下载等待时间
TIMME_UPLOADER_PROCESSING = 60
S_UPLOADER_DIR = "/static/uploadings"
S_FILES_DIR = "/static/files"

class Uploader(BaseHandler):
    def _name(self, name): 
        uid = self.get_current_user() 
        md5 = hashlib.md5()
        md5.update(uid + name)
        return md5.hexdigest()

    def _expectations(self, size, name):
        # 还可以提前划分子上传块， 每次请求给一个， 并标记上传中， 完成删除, 但是浪费内存 1T / 4MB 
        #[0-4] [1-5] [2-6] 这类型怎么处理 
        expectations = [[0, MAX_UPLOADER_SIZE], [MAX_UPLOADER_SIZE,size]] # 开始的上传时候
        d_path = os.path.join(S_UPLOADER_DIR ,name)
        files = os.listdir(d_path) # 不存在文件夹， logger error
        r = re.compile("^\d+-\d+$")
        f = lambda x: r.findall(x) 
        files = filter(f, files)
        if not files:
            return expectations
        f = lambda x: map(int, x.split("-"))
        files = map(f, files)
        files = sorted(files)
        for l in files:
            old_expectations = expectations  
            for idx in xrange(len(old_expectations)):
                e = old_expectations[idx]
                if e in files:
                    expectations = old_expectations[:idx] + old_expectations[idx+1:]
                    continue
                if l[0] > l[1]: # 文件名不对
                    expectations = old_expectations[:idx] + old_expectations[idx+1:]
                    # loggger.error("file:%s中的文件不对：%s"%(name, e))
                    break 
                if e[0] <= l[0] and l[1] <= e[1]:
                    a, b = l[0]-e[0], e[1] - l[1]
                    new = []
                    if a > 0:
                        new.append([e[0], l[0]])
                    if b > 0:
                        new.append([l[1], e[1]])
                    if new:
                        expectations = old_expectations[:idx] + old_expectations[idx+1:]
                        expectations.extend(new)
                    break
        return sorted(expectations)

    def __expectations(self, size, name="c431d9c42836ac66cd26af0918cdffc2"):
        # 虽然稍快， 鲁棒性有点差, 废弃了， 不能修改 MAX_UPLOADER_SIZE 的大小,  
        expectations = ["0/%s"%size]
        d_path = os.path.join(S_UPLOADER_DIR ,name)
        if not os.path.exists(d_path):
            return expectations
        files = os.listdir(d_path)
        if not files:
            return expectations
        expectations = []
        if files:
            files = sorted(files)
            f = lambda x: x.split("-")
            l = map(f, files)
            max_file = sorted(files, cmp=cmp_file_name)[-1]
            start, end = map(int, max_file.split('-'))
            expectations = [str(end)+"-"+str(size)]
            for i in xrange(0, end, MAX_UPLOADER_SIZE):
                expectations.append(str(i) + '-' + str(i+MAX_UPLOADER_SIZE))
            expectations = list(set(expectations) - set(files))
        return sorted(expectations, cmp=cmp_file_name)

    def _next_uploading(self, f_name):
        name = self._name(f_name) 
        r = redis.Redis() 
        k_expectations = PRE_UPLOADER + "expectation:%s" % name
        k_processings = PRE_UPLOADER + "processing:%s" % name 
        rt = {"code": 0, "msg":  u"等会再来吧", "time":10}
        expectations = r.smembers(k_expectations)
        processings = r.hgetall(k_processings)
        expectations = sorted(expectations)
        # 万一(processing, exceptions 太长)轮询太长了，链接会断开的, 可能吗? 可能的话封装redis.operstor
        for e2, processing in processings.items(): # 先传过期的
            if time.time() - float(processing) > TIME_UPLOADER_WAITING:  
                start, end = map(int, e2.split("-"))
                rt = {"code":0, "expectation": [start, end] }
                r.hset(k_processings, e2, time.time())
                r.expire(k_processings, TIMME_UPLOADER_PROCESSING) #  
                break  
        else: #上传中的文件块没有过期的
            for e1 in expectations:
                if e1 not in processings.keys(): # 跳过上传中的
                    start, end = map(int, e1.split("-"))
                    if end-start > MAX_UPLOADER_SIZE: # 该块太大了， 切割下
                        idx = expectations.index(e1)
                        expectations = expectations[:idx] + expectations[idx+1:] 
                        e1 = str(start)+"-"+str(start+MAX_UPLOADER_SIZE)
                        e2 = str(start+MAX_UPLOADER_SIZE)+"-"+str(end)
                        expectations.extend([e1, e2])
                        expectations = set(expectations)
                        r.delete(k_expectations)
                        r.sadd(k_expectations, *expectations)    
                        start, end = start, start + MAX_UPLOADER_SIZE
                    r.hset(k_processings, e1, time.time()) 
                    # 如果每个子块都没有上传成功， 还不停的请求任务， 是有bug, redis 不会过期了
                    r.expire(k_expectations, TIME_UPLOADER_EXPECTATIONS)
                    r.expire(k_processings, TIMME_UPLOADER_PROCESSING) #  
                    rt = {"code":0, "expectation": [start, end] }
                    break
            else: # redis 中没有期望上传块了, 1: 上传完了; 2: key 过期了 
                rt = {"code":2, "msg":"上传完成"}  
                conn = MySQLdb.connect(**conf.mysql)
                conn.set_character_set('utf8')  
                cur = conn.cursor()   
                sql = "select size, integerity from files where _name='%s' limit 1" % (name)
                cur.execute(sql)
                f = cur.fetchone()
                if not f:
                    # err = "%s, 是否存在， 数据中不存在" % (name)
                    #logger.error(err) 
                    rt = {"code":-1, "msg":"请先创建资源"}
                elif f[1] == 100: # 这里会有一次小的高并发， 因为客户端是多线程并发上传的(暂不处理)
                    r.delete(k_expectations)
                    r.delete(k_processings)
                else:# 没上传完
                    size = f[0]     
                    expectations = self._expectations (size, name) 
                    integerity = 100 - sum([e[1]-e[0] for e in expectations]) * 100 / size
                    if not expectations or 100 == integerity:
                        sql = 'update files set integerity=100 where _name="%s"' % name 
                        #  merge files
                        cur.execute(sql)
                        conn.commit() 
                    else:
                        start, end = expectations[0] 
                        if end-start > MAX_UPLOADER_SIZE:
                            expectations = [start, MAX_UPLOADER_SIZE] + [MAX_UPLOADER_SIZE, end]  + expectations[1:] 
                        expectations = ["%s-%s"%(e[0],e[1]) for e in expectations] 
                        r.sadd(k_expectations, *expectations)
                        r.hset(k_processings, expectations[0], time.time())
                        r.expire(k_processings, TIMME_UPLOADER_PROCESSING ) #  
                        r.expire(k_expectations, TIME_UPLOADER_EXPECTATIONS)
                        rt = {"code":0, "expectation": expectations[0].split('-') }
        self.j_write(rt)   

    #@authenticated
    def get(self, f_name):
        ''' 返回上传状态, 那还需要在post成功时 中记录? 
        conn = MySQLdb.connect(host='localhost',user='root',passwd='',db='upload_files')
        conn.set_character_set('utf8')  
        cur = conn.cursor()   
        sql = "select title, integerity from files where _name='%s' limit 1" % (name)
        cur.execute(sql)
        file = cur.fetchone()
        if file:
             rt = {}   
        '''  
        self._next_uploading(f_name)

    def put(self, f_name):
        title = self.get_argument("title")
        size = self.get_argument("size")
        name = self._name(f_name) 
        owner_id = self.get_current_user() 
        try:
            size = int(size)
        except :
            raise MissingArgumentError("参数丢失")
        sql = "insert ignore into files(`_name`, `title`, `owner_id`, `size`) "\
              "values ('%s', '%s', '%s', %s)"
        sql = sql % (name, MySQLdb.escape_string(title), owner_id, size)
        conn = MySQLdb.connect(**conf.mysql)
        conn.set_character_set('utf8')  
        cur = conn.cursor()   
        id = cur.execute(sql)
        conn.commit()
        d_path = os.path.join(S_UPLOADER_DIR, name)
        exists = os.path.exists(d_path)
        if not exists:
            os.mkdir(d_path)
            if not id: 
                #logger.error("文件夹被删除了:%s"%d_path)
                pass
        elif not id:
            sql = 'select size, title from files where _name="%s" limit 1'% name 
            cur.execute(sql)
            file = cur.fetchone()
            if not file:
                raise Exception("%s, 文件夹存在， 数据库中不存在"%(name))  
            if file[0] != size or title != file[1]:
                err = "%s 数据库 title:%s, size:%s， 上传：title:%s, name:%s"%(f[1], f[0], title, size) 
                raise Exception(err)  
            # logger.error("数据库中存在 file:%s， 但是文件不见了"%(name))
        expectations = ["%s-%s"%(a, b) for a, b in [(0, MAX_UPLOADER_SIZE),(MAX_UPLOADER_SIZE, size)]]
        r = redis.Redis()
        k_expectations = PRE_UPLOADER + "expectation:%s" % name
        k_processings = PRE_UPLOADER + "processing:%s" % name 
        r.sadd(k_expectations, *expectations)
        r.hset(k_processings, expectations[0], time.time())
        r.expire(k_processings, TIMME_UPLOADER_PROCESSING ) #  
        r.expire(k_expectations, TIME_UPLOADER_EXPECTATIONS)
        rt = {"code":1, "msg":"开始上传", "expection":[0, MAX_UPLOADER_SIZE]} 
        self.j_write(rt)

    def post(self, f_name):
        name = self._name(f_name) 
        k_expectations = PRE_UPLOADER + "expectation:%s" % name
        k_processings = PRE_UPLOADER + "processing:%s" % name 
        start = self.get_argument("start")
        end = self.get_argument("end")
        try:
            int(start)
            int(end)
        except:
            raise MissingArgumentError("丢失格式不对, start:%s, end:%s"%(start, end))
        block = "-".join([start, end])
        if not r.sismember(k_expectations, block): # 不要乱传(还有redis 有可能过期了)
            return self._next_uploading(f_name) 
        f_path = os.path.join(S_UPLOADER_DIR, name, block) 
        if os.path.exists(f_path):
            # 文件存在 
            #logger.error("文件已经存在了：%s"%(f_path))
            r = redis.Redis()
            r.hdel(k_processings, block)
            r.srem(k_expectations, block)
            return self._next_uploading(f_name)
        f = open(f_path, "w+")
        if not f:
            # deadly error
            raise Exception("创建文件失败%s"%f_path)  
        data = self.get_argument("data")
        #if len(data) != int(end)-int(start) : # 大小不够
            #return self._next_uploading(f_name)
        f.write(data)
        f.close()
        r = redis.Redis()
        r.hdel(k_processings, block)
        r.srem(k_expectations, block)
        self._next_uploading(f_name)


class Downloader(BaseHandler):
    def _name(self, name): 
        uid = self.get_current_user() 
        md5 = hashlib.md5()
        md5.update(uid + name)
        return md5.hexdigest()

    def get(self, f_name):
        ip = self.request.remote_ip
        id = self.get_current_user() 
        name = self._name(f_name)
        md5 = hashlib.md5()
        md5.update(ip + id + f_name)
        sid = md5.hexdigest()
        k_downloader = PRE_DOWNLOADER + "prcessing:" + sid # 同时下载多个文件
        r = redis.Redis()
        pos = r.get(k_downloader)
        data = "" 
        if not pos: # 这里判断下所属权利
            sql = "select size from files where _name='%s' and owner_id=%s limit 1" %(name, id)
            conn = MySQLdb.connect(**conf.mysql)
            conn.set_character_set('utf8')  
            cur = conn.cursor() 
            cur.execute(sql)     
            file = cur.fetchone()
            if not file: # 无权访问
                self.set_status(404)
                return
            pos = 0
        pos = int(pos)
        f_path = os.path.join(S_FILES_DIR, name) 
        f = open(f_path)  
        if not f:
            err = "file:%s not exists"%f_path
            raise Exception(err) 
        f.seek(pos)
        data = f.read(MAX_DOWNLOADER_SIZE)
        f.close()
       # self.set_header ('Content-Type', 'application/octet-stream')
        rt = '{"code": 0, "data": "%s"}' % data
        try:
            r.set(pos + MAX_DOWNLOADER_SIZE)
        except:
            r = redis.Redis()
            r.set(k_downloader, pos+MAX_DOWNLOADER_SIZE) 
        r.expire(k_downloader, TIME_DOWNLOADER)
        if len(data) < MAX_DOWNLOADER_SIZE:
            r.delete(k_downloader)
            rt = '{"code": 1, "data": "%s"}' % data
        self.write(rt)  
