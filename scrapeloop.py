import os, sys, csv, thread, time
import sqlite3 as lite
from Queue import Queue, LifoQueue
import logging

from threading import Thread

logger = logging.getLogger('scrapeloop')
log = logger.warning


class DbStuff:
    m_db_name = ""
    filename_failed_urls = "failed_urls.txt"

    def __init__(self, dbname, csv_file_name, csv_col_names, overwritedata = True):
        self.m_db_name = dbname
        self.m_db_lock = thread.allocate_lock()  # Condition()

        if overwritedata:
            if os.path.exists(csv_file_name):
                os.remove(csv_file_name)
            if os.path.exists(dbname):
                os.remove(dbname)
            if os.path.exists(self.filename_failed_urls):
                os.remove(self.filename_failed_urls)

        # csv_file_name = "results.csv"
        self.m_csv_file = open(csv_file_name, 'a+')

        if not csv_col_names:
            self.m_csv_writer = None
        else:
            self.m_csv_writer = csv.writer(self.m_csv_file)

            csv_size = os.path.getsize(csv_file_name)
            if csv_size == 0:
                self.m_csv_writer.writerow(csv_col_names)

        self.m_total_done = 0

    def close(self):
        if not self.db_con is None:
            self.db_con.commit()
            self.db_con.close()
        if not self.m_csv_file is None:
            self.m_csv_file.close()

    def csv_save_item(self, item, url_fingerprint, encode=True):

        try:

            self.m_db_lock.acquire()

            if self.__db_exist_entry_no_lock(url_fingerprint):
                return

            if self.m_csv_writer:

                for i in range(len(item)):
                    if item[i] == "--":
                        item[i] = ''

                if encode:
                    self.m_csv_writer.writerow([s.encode("utf-8") for s in item])
                else:
                    self.m_csv_writer.writerow(item)

                self.m_csv_file.flush()
            else:
                if encode:
                    item = item.encode("utf-8")
                self.m_csv_file.write(item + "\r\n")

                self.m_csv_file.flush()

            self.__sql_add_url_fingerprint_no_lock(url_fingerprint)

            self.m_total_done += 1

        finally:
            self.m_db_lock.release()

        return

    def db_create(self):

        self.db_con = lite.connect(self.m_db_name, check_same_thread=False)

        # with con:
        cur = self.db_con.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS Entries (Fingerprint Text PRIMARY KEY)")

        return

    def __sql_add_url_fingerprint_no_lock(self, Fingerprint):

        try:
            cur = self.db_con.cursor()

            cur.execute("INSERT INTO Entries(Fingerprint) VALUES(?)", (Fingerprint,))

            self.db_con.commit()

        except Exception as ex:
            log(str(ex))
            return False

        return True

    def __db_exist_entry_no_lock(self, fingerprint):

        # con = lite.connect(self.m_db_name)
        posts = []
        # with con:
        cur = self.db_con.cursor()

        cur.execute("Select * from Entries where Fingerprint=?", (fingerprint,))

        data = cur.fetchone()
        if (data and data[0]):
            return True

        return False

    def db_exist_entry(self, fingerprint):
        res = None

        try:
            self.m_db_lock.acquire()

            res = self.__db_exist_entry_no_lock(fingerprint)

        finally:
            self.m_db_lock.release()

        return res

    def add_failed_url(self, url):
        with open(self.filename_failed_urls, "a+") as f:
            f.write(url + "\r\n")


class QueueItem(object):
    def __init__(self, url, callback, iter_count=0, param=None):
        self.url = url
        self.callback = callback
        self.iter_count = iter_count
        self.param = param


class Worker(Thread):
    def __init__(self, queue, param, thread_id, worker_timeout, db, max_retries):
        Thread.__init__(self)
        self.queue = queue
        self.param = param
        self.worker_index = "worker" + str(thread_id)
        self.log_prefix = self.worker_index + ": "
        self.worker_timeout = worker_timeout
        self.queue_item = None
        self.db = db
        self.max_retries = max_retries
        self.url = ""

    def log(self, s):
        log(self.log_prefix + s)

    def add_queue_item(self, url, callback, param=None):
        self.queue.put(QueueItem(url=url, callback=callback, param=param))

    def run(self):
        try:
            while True:
                queue_item = self.queue.get()
                if queue_item == None:
                    self.queue.put(None)  # Notify the next worker
                    log(str(self.worker_index) + " no more tasks, exit")
                    break

                self.queue_item = queue_item
                self.url = queue_item.url

                queue_item.callback(self)

                self.queue.task_done()

                if self.worker_timeout:
                    time.sleep(self.worker_timeout)

        except Exception as ex:
            self.log("FATAL ERROR worker")
            self.log(str(ex))

    def retry_queue_item(self):

        if self.queue_item.iter_count < self.max_retries:

            self.queue_item.iter_count+=1
            self.queue.put(self.queue_item)
            self.log("retry #" + str(self.queue_item.iter_count) + " " + self.queue_item.url)
        else:
            self.log("FATAL ERROR " + self.queue_item.url)
            self.db.add_failed_url(self.queue_item.url)


class ScrapeLoop(object):
    db = None
    queue = None
    workers = []

    def __init__(self, csv_colnames, start_urls, dbpath="profiles.db", csvpath="result.csv", fifo = True,
                 worker_params = [], worker_timeout = 0, overwritedata = True, max_retries = 10):

        self.queue = Queue() if fifo else LifoQueue()

        self.db = DbStuff(dbpath, csvpath, csv_colnames, overwritedata)
        self.db.db_create()

        for url_tuple in start_urls:
            url, callback = url_tuple
            self.queue.put(QueueItem(url=url, callback=callback))

        for i, worker_param in enumerate(worker_params):
            w = Worker(self.queue, worker_param,  i+1, worker_timeout, self.db, max_retries)
            self.workers.append(w)

    def start_workers_and_wait(self, single_thread=False):

        if single_thread:
            self.workers[0].run()
            log("job finished")
            self.db.close()
            return

        for w in self.workers:
            w.start()

        log("waiting workers")

        self.queue.join()
        log("all tasks done")

        self.queue.put(None)

        for w in self.workers:
            w.join()

        log("job finished")
        self.db.close()
