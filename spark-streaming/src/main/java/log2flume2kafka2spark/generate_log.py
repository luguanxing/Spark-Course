import random
import time

url_paths = [
    "class/100.html",
    "class/101.html",
    "class/102.html",
    "class/250.html",
    "class/251.html",
    "class/205.html",
    "class/660.html",
    "class/666.html",
    "class/667.html",
    "class/668.html",
    "learn/123",
    "learn/456",
    "learn/789",
    "course/list"
]

ip_slices = [123, 121, 10, 122, 75, 133, 68, 90, 55, 182, 175, 163]

http_referers = [
    "https://www.baidu.com/s?wd={query}",
    "https://www.sogou.com/web?query={query}",
    "http://cn.bing.com/search?q={query}",
    "https://search.yahoo.com/search?p={query}"
]

search_keywords = [
    "Spark SQLs实战",
    "Spark Streaming实战",
    "Hadoop",
    "大数据面试"
]

status_codes = ["200", "200", "200", "200", "200", "200", "200", "200", "500"]


def random_url():
    return random.sample(url_paths, 1)[0]


def random_ip():
    slice = random.sample(ip_slices, 4)
    return ".".join(str(item) for item in slice)


def random_referer():
    if random.uniform(0, 1) >= 0.3:
        return "-"
    refer_str = random.sample(http_referers, 1)[0]
    query_str = random.sample(search_keywords, 1)[0]
    return refer_str.format(query=query_str)


def random_status():
    return random.sample(status_codes, 1)[0]


def generate_log(count=10):
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    f = open("/Users/luguanxing/data/access.log", "a+")

    while count >= 1:
        query_log = "{ip}\t{time}\t\"GET /{url} HTTP/1.1\"\t{status_code}\t{referer}".format(time=time_str, url=random_url(), ip=random_ip(), referer=random_referer(), status_code=random_status())
        print(query_log)
        f.write(query_log + "\n")
        count = count - 1


if __name__ == '__main__':
    while True:
        generate_log(10)
        time.sleep(1)
