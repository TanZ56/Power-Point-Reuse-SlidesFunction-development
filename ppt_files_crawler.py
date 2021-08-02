import argparse
from multiprocessing import Queue
import time
from multiprocessing import Process, Queue, Lock, Manager
import os
import json
import urllib
import requests
from urllib.parse import urlparse
import hashlib
import string

def info(title):
    '''
    Print log with thread information.
    '''
    print(title, end='')
    print(' module name:', __name__, end='')
    if hasattr(os, ' getppid'):
        print(' parent process:', os.getppid(), end='')
    print(' process id:', os.getpid(), end='\n')

def get_host(url):
    '''
    Return hostname of url.
    '''
    o = urlparse(url)
    return o.hostname

def get_file_name(url, url_md5):
    '''
    Name the file with md5 and get extention from url.
    '''
    if url.lower().find('.pptx') > 0:
        file_name = url_md5 + '.pptx'
    elif url.lower().find('.ppt') > 0:
        file_name = url_md5 + '.ppt'
    else:
        file_name = url_md5
    return file_name

class UrlItem(object):
    '''
    UrlItem for task queue.
    '''
    url: string
    doc_url: string
    host: string
    file_name: string

headers = {
    "Accept" : "*/*",
    "User-Agent" : "Mozilla/5.0 (Windows NT x.y; rv:10.0) Gecko/20100101 Firefox/10.0"
}

def download(item, path, url_404):
    try:
        unescaped_url = urllib.parse.unquote(item.doc_url)

        if not os.path.exists(path):
            os.makedirs(path)

        final_path = os.path.join(path, item.file_name)
        if not os.path.exists(final_path):
            r=requests.get(unescaped_url, timeout=30, headers=headers)
            if r.status_code == 200 and r.headers['Content-Type'].find('html') < 0: # ignore smal files 1k
                print('Download', item.doc_url)
                with open(final_path, 'wb') as output:
                    output.write(r.content)
            else:
                r=requests.get(item.doc_url, timeout=30, headers=headers)
                if r.status_code == 200 and r.headers['Content-Type'].find('html') < 0: # ignore smal files 1k
                    print('Download', item.doc_url)
                    with open(final_path, 'wb') as output:
                        output.write(r.content)
                else:
                    print('[' + str(r.status_code) + '] ', item.doc_url)
                    url_404[item.doc_url]=item.file_name
    except Exception as e:
        print('[exception] ', item.doc_url)
        url_404[item.doc_url]=item.file_name
        print(e)

# host status shared across multi process
def acquire(host_concurrency, host):
    if (not host in host_concurrency.keys()) or (host_concurrency[host] == False):
        host_concurrency[host] = True
        return True
    else:
        return False

def release(host_concurrency, host):
    host_concurrency[host] = False

def worker(seq, host_concurrency, url_queue, path, url_404):
    info('workder')
    idle = 0
    done = False
    while not done:
        if not url_queue.empty():
            idle = 0
            item = url_queue.get()

            if(acquire(host_concurrency, item.host)):
                download(item, path, url_404)
                release(host_concurrency, item.host)
            else:
                url_queue.put(item)
        else:
            info('q is empty')
            idle += 1
            time.sleep(1)
            if idle > 2:
                done = True

def producer(url_queue, input_file, path, url_404):
    info('producer')
    if not os.path.exists(path):
        os.makedirs(path)
    with open(input_file, 'rb') as input_stream:
        url_count=0
        for line in input_stream:
            if(url_queue.full()):
                time.sleep(10)

            url_count += 1
            cols = line.decode('utf-8').strip().split('\t')
            if not len(cols) == 3:
                print('Expecting 3 columns of file ', input_file)
                break
            url = cols[0]
            doc_url = cols[1]
            url_md5 = cols[2]
            file_name = get_file_name(url, url_md5)

            item = UrlItem()
            item.url = url
            item.doc_url = doc_url
            item.host = get_host(url)
            item.file_name = file_name

            # skip exist files
            final_path = os.path.join(path, file_name)
            if not os.path.exists(final_path) and (doc_url not in url_404.keys()):
                url_queue.put(item)

if __name__ == '__main__':
    r'''
    python ppt_files_crawler.py -t 1 -i E:\data\pptSideStream_Tire_0_en.tsv -o E:\data\tier0
    '''
    info('main')

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input file', required=True, default='')
    parser.add_argument('-t', '--threadn', help='thread number', required=True, default=100)
    parser.add_argument('-o', '--output', help='output folder', required=True, default='')
    args = vars(parser.parse_args())
    input_file = args['input']
    thread_num = int(args['threadn'])
    output_folder = args['output']

    proc_arr = []
    url_queue = Queue()

    manager = Manager()
    host_concurrency = manager.dict()
    url_404 = manager.dict()

    # generate URLs
    file_name = input_file + ".url_404.json"
    if os.path.exists(file_name):
        try:
            url_404=json.load(open(file_name))
        except Exception as e:
            print(e)

    p = Process(target=producer, args=(url_queue, input_file, output_folder, url_404))
    p.start()
    proc_arr.append(p)

    # multiple work consume URLs
    for i in range(thread_num):
        p = Process(target=worker, args=(i, host_concurrency, url_queue, output_folder, url_404))
        p.start()
        proc_arr.append(p)

    for p in proc_arr:
        p.join()

    with open(input_file+".url_404.json", 'w') as output:
        json.dump(url_404.copy(), output)

    info('done')