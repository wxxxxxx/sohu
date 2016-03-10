#! -*- coding:utf8 -*-
import os, re

MAX_UPLOADER_SIZE = 4

def cmp_file_name(a, b):
     l1 = map(int, a.split("-"))
     l2 = map(int, b.split("-"))
     return cmp(l1, l2) 

class Obj(object):
    def exceptions(self, size, name = "c431d9c42836ac66cd26af0918cdffc2"):
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
         
    def _exceptions(self, size, name="c431d9c42836ac66cd26af0918cdffc2"):
        exceptions = ["0/%s"%size]
        d_path = os.path.join("static",name)
        if not os.path.exists(d_path):
            return exceptions
        files = os.listdir(d_path)
        if not files:
            return exceptions    
        exceptions = []         
        r = re.compile("^\d+-\d+$")   
        files = sorted(files)
        f = lambda x: x.split("-")
        l = map(f, files) 
        max_file = sorted(files, cmp=cmp_file_name)[-1] 
        start, end = map(int, max_file.split('-'))
        exceptions = [str(end)+"-"+str(size)]
        if end > size:
             end = size
             exceptions = []
        for i in xrange(0, end, MAX_UPLOADER_SIZE):
            exceptions.append(str(i) + '-' + str(i+MAX_UPLOADER_SIZE))
        exceptions = list(set(exceptions) - set(files)) 
        return sorted(exceptions, key= lambda x: x[1])  
if __name__ == "__main__":
    obj = Obj()
    size = 13
    exceptions = obj.exceptions(size)
    integerity = sum([e[1]-e[0] for e in exceptions]) * 100  / size
    print integerity
    print exceptions
    size = 10
    exceptions = obj.exceptions(size)
    integerity = 100 -sum([e[1]-e[0] for e in exceptions]) * 100  / size
    print integerity
    print exceptions
    size = 100
    exceptions = obj.exceptions(size)
    integerity = 100 -sum([e[1]-e[0] for e in exceptions]) * 100  / size
    print integerity
    print exceptions
    size = 22
    exceptions = obj.exceptions(size)
    integerity = 100 - sum([e[1]-e[0] for e in exceptions]) * 100  / size
    print integerity
    print exceptions
