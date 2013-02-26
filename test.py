#!/usr/bin/python
#coding=utf-8

from threadpool import ThreadPool

from comment import *
import Queue
import hashlib

def run(arg1):
    print 'xxxxxxxxxxxxxx'+'\n'
	
	
poolSize = 100
poolModel = 'exit_inner_msg'
tp = ThreadPool(poolSize,poolModel)
for i in range(100):
    tp.putToQueue(run,arg1=i)

tp.waitforComplete()


rooturl='http://www.sygscls.com/'

urlQueue = Queue.Queue()

urlQueue.put(rooturl)
def get_url_from_html(url):
    res=HTTP.getPage(url=url)
    if res[2]==200:
        urls=getUrls(res[0],url)
        return urls
    else:
        return 

'''		
a=get_url_from_html(rooturl)
print a
'''



urls=[]	
visited = []

while urlQueue.empty()!=True:
	url=urlQueue.get()
	reurls=get_url_from_html(url)
	if reurls:
		for url in reurls:
			#md5����url���̶����ȣ���Լ�ڴ�
			if hashlib.md5(url).hexdigest() in visited:
				continue
			#�ڶ�url�������ӵ�֮ǰ�Ͱ����url��������������޷������url�����������urlʱ�Ͳ�����ȥ����һ����
			else:
				visited.append(hashlib.md5(url).hexdigest())
				urlQueue.put(url)
				urls.append(url)

for i in urls:
	print i
				
		
       
    
