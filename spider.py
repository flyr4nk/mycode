#-*- coding: utf-8 -*-
#参数列表：-u -d -f -l --dbfile --thread --testself --key

#欠缺
#英文注释
#正则表达式
#异常判断改进
#logging改进
#更多的应用机会以发现bug

#bs4:beautifual soup4
import getopt, sys, re, urllib, urllib2, threading, Queue, bs4, time, chardet, sqlite3, datetime
import logging
import traceback
import pdb
from comment import *
from threading import stack_size

stack_size(32768*16)

LOG_LEVEL = {'1':logging.DEBUG,
        '2':logging.INFO,
        '4':logging.ERROR,
        '3':logging.WARNING,
        '5':logging.CRITICAL,
        }
#urlparse的解析方式
#scheme://netloc/path;parameters?query#fragment

#将获得的HTML转化为utf-8格式的字符串
unicode_dict = {
         'GB2312': lambda c : unicode(c, 'GB2312', 'ignore').encode('utf-8', 'ignore'),
        'Big5': lambda c : unicode(c, 'Big5', 'ignore').encode('utf-8', 'ignore'),
        'ascii': lambda c : unicode(c, 'ascii', 'ignore').encode('utf-8', 'ignore'),
        'GBK': lambda c : unicode(c, 'GBK', 'ignore').encode('utf-8', 'ignore'),
        'UTF-8': lambda c : c,
        'utf-8': lambda c: c,
        }

#日志记录logger的初始化和获取函数
logger = None
def init_logger(log_file, log_level):
    global logger
    if not logger:
        logger = logging.getLogger()
        hdlr = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(log_level)

def get_logger():
    global logger
    return logger

#连接sqlite3数据库
def get_db_conn(db_name):
    try:
        return sqlite3.connect(db_name)
    except:
        logger = get_logger()
        logger.error(u'链接数据库失败')
        traceback.print_exc()

#爬虫线程
class SpiderThread(threading.Thread):
    def __init__(self, pool, *args, **kwargs):
        threading.Thread.__init__(self)
        self.pool = pool
        self.setDaemon(True)
        slef.visited=[] #fang wen guo de url
        #在初始化后自动开始执行
        self.start()

    def run(self):
        while True:
            #获取日志记录接口
            logger = get_logger()
            try:
                logger.debug(self.getName() + u'获取参数...')
                #从任务队列获取一个任务
                call, args, kwargs = self.pool.get_job()

                logger.debug(self.getName() + u'调用处理方法...')
                #执行方法
                res, urls = call(*args, **kwargs)
                logger.debug(self.getName() + u'增加结果队列...')
                #将结果添加到结果队列
                if res:
                    self.pool.add_result(res)

                logger.debug(self.getName() + u'添加新任务...')
                #添加新任务
                if urls:
                    for i in urls:
                        if hashlib.md5(i).hexdigest() in visited:
				continue
			else:
				visited.append(hashlib.md5)
                logger = get_logger()
                logger.info(self.getName() + u': ' + args[0] + u' 完成 Deep:' + str(kwargs['cur_deep']))

            #如果在设定的timeout值的时间之内没有获得新任务，判定没有新任务，线程结束
            except Queue.Empty:
                with self.pool.lock:
                    self.pool.thread_url[self.getName()] = ''
                logger = get_logger()
                logger.info(self.getName() + ' End!')
                break

            #捕获异常
            except Exception as e:
                with self.pool.lock:
                    self.pool.error_num += 1
                logger = get_logger()
                logger.error(self.getName() + ': '+ str(type(e)) + '!\n' + ' url:' + args[0] + ' parent_url:' + args[1])
            except urllib2.URLError:
                with self.pool.lock:
                    self.pool.error_num += 1
                logger = get_logger()
                logger.error(self.getName() + ': URLERROR!\n' + ' url:' + args[0] + ' parent_url:' + args[1])

#检测线程池状态的类
class ThreadPoolStatus(threading.Thread):
    def __init__(self, pool):
        threading.Thread.__init__(self)
        #持有线程池实体
        self.pool = pool
        self.setDaemon(True)

    #线程run方法，没10秒打印一次进度信息。
    def run(self):
        while True:
            time.sleep(10)
            print 'work queue :', self.pool.work_queue.qsize()
            print 'result queue :', self.pool.result_queue.qsize()
            thread_num = 0
            for i in self.pool.threads:
                if i.is_alive():
                    thread_num += 1
            eprint 'thread number:', thread_num
            print 'exception :', self.pool.error_num
            print

#线程池
class ThreadPool:
    def __init__(self, thread_class, num=10, timeout=30):
        self.work_queue = Queue.Queue()
        self.result_queue = Queue.Queue()
        self.threads = []
        self.timeout = timeout
        self.thread_class = thread_class
        self.other_threads = []
        self.__create_pool(num)
        self.lock = threading.RLock()
        self.error_num = 0

    #创建线程池
    def __create_pool(self, num):
        for i in range(num):
            thread = self.thread_class(self)
            self.threads.append(thread)

    #等待线程池完成任务
    def wait_for_complete(self):
        while len(self.threads):
            thread = self.threads.pop()
            if thread.isAlive():
                thread.join()

    #添加任务，获取任务以及获得处理结果列表的方法
    def add_job(self, job):
        self.work_queue.put(job)

    def get_job(self):
        return self.work_queue.get(timeout=self.timeout)

    def add_result(self, url):
        self.result_queue.put(url)

    def get_result_list(self):
        return self.result_queue

    #添加另外的运行线程，这里主要指的是监视线程池状态的线程
    def add_threads(self, thread):
        thread.start()
        self.other_threads.append(thread)
	
#爬虫类
class Spider():
    def __init__(self, **kwargs):
        self.url = kwargs.get('-u', None)
        self.deep = int(kwargs.get('-d', 5))
        self.logfile = kwargs.get('-f', 'log.txt')
        self.log_level = kwargs.get('-l', '1')
        self.dbfile = kwargs.get('--dbfile', 'db')
        self.thread = int(kwargs.get('--thread', '5'))
        self.testself = '--testself' in kwargs
        self.key = kwargs.get('--key', None)

        if not self.url:
            print u'请输入 -u url'
            sys.exit()

        if not self.key:
            print u'请输入 --key 关键字'
            sys.exit()

    #爬虫方法可以传递给线程池分配任务
    def spider_method(self, *args, **kwargs):
        url, parent_url = args
        cur_deep = kwargs.get('cur_deep', None)
        deep = kwargs.get('deep', None)
        #key = kwargs.get('key', None)
        '''
        #判断是否读取页面
        if url and deep and key and cur_deep <= deep:
            #获取网页
            soup = self.get_html(url, parent_url)
            res = None
            if soup:
                #简单的判断是否包含关键字
                if key.decode('utf-8') in soup.get_text():
                    res = url
                kwargs['cur_deep'] += 1
                #生成新任务
                new_job = [(self.spider_method, (i.get('href'), url), kwargs) for i in soup.findAll('a') if i.get('href', None) != None]
                return res, new_job
        return None, []
        '''
        html=self.get_html(url)
	if html:
		urls=self.get_url_from_html(html)
		if urls:
		    return url,urls
     			
    def get_url_from_html(html):
	urls=getUrls(html)
        return urls

    #获取html，简单分析url，并读取页面
    def get_html(self, url, parent_url):
        '''
        url_parse_result = urllib2.urlparse.urlparse(url)
        url_parent_parse_result = urllib2.urlparse.urlparse(parent_url)
        content = ''
        res = None
        if url_parse_result.scheme and url_parse_result.netloc:
            res = urllib2.urlopen(url, timeout=30)
        elif url_parse_result.path:
            res = urllib2.urlopen(parent_url + '/' + url_parse_result.path)
        if res:
            content = res.read()
            #利用dict模拟switch来将页面编码变为utf-8
            content = unicode_dict[chardet.detect(content)['encoding']](content)
            return  bs4.BeautifulSoup(content)
        else:
            return None
        '''
	res=HTTP.getPage(url=url)
	if res[2]==200:
		content = unicode_dict[chardet.detect(res[0])['encoding']](content)
		return bs4.BeautifulSoup(content)
	else:
            return None 

    def run(self):
        init_logger(self.logfile, LOG_LEVEL[self.log_level])
        pool = ThreadPool(SpiderThread, self.thread, 60)
        pool.add_threads(ThreadPoolStatus(pool))
        pool.add_job((self.spider_method, (self.url, ''), dict(cur_deep=1, deep=self.deep, key=self.key)))
        pool.wait_for_complete()

        #链接数据库
        db_table_name = 'table_'
        db_table_name += datetime.datetime.now().strftime('%Y%m%d%I%M%S')
        conn = get_db_conn(self.dbfile)
        cursor = conn.cursor()
        cursor.execute('create table %s(url text, key text)' % db_table_name)

        logger = get_logger()
        logger.info(u'写入数据库')
        result = pool.result_queue
        while not result.empty():
            item = result.get()
            cursor.execute('insert into %s(url, key) values("%s", "%s")' % (db_table_name, item, self.key))
        logger.info(u'写数据库完成')
        conn.commit()

def get_arg_dict(l):
    try:
        opts, args = getopt.getopt(l, 'u:d:f:l:', ['dbfile=', 'thread=', 'testself', 'key='])
        if args:
            ##print 'to many arguments : ', args
            sys.exit()
        elif not opts:
            print u'参数列表：-u -d -f -l --dbfile --thread --testself --key'
        else:
            return dict(opts)
    except getopt.GetoptError:
        logger = get_logger()
        logger.error(u'参数不正确')
        sys.exit()
    pass

def start(arg=sys.argv[1:]):
    arg_dict = get_arg_dict(arg)
    spider = Spider(**arg_dict)
    spider.run()

if __name__ == '__main__':
    try:
        start()
    except KeyboardInterrupt:
        sys.exit()


