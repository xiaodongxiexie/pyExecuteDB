# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/7/20


import re
import json

import requests
from lxml.etree import HTML


def get_headers(headers: str) -> dict:
    response = {}
    for header in headers.split("\n"):
        if not header.strip():
            continue
        k, v = header.split(":", maxsplit=1)
        k, v = k.strip(), v.strip()
        response[k] = v
    return response


if __name__ == '__main__':

    headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9
Cache-Control: max-age=0
Connection: keep-alive
Cookie: BAIDU_SSP_lcr=https://www.dogedoge.com/results?q=%E4%B8%AD%E5%9B%BD%E7%A5%A8%E6%88%BF%E6%8E%92%E8%A1%8C; BAIDU_SSP_lcr=https://www.dogedoge.com/results?q=%E4%B8%AD%E5%9B%BD%E7%94%B5%E5%BD%B1%E7%A5%A8%E6%88%BF%E6%8E%92%E8%A1%8C; BAIDUID=8DA8504A71DF0BAA50DA29D0AFAD6862:FG=1; BIDUPSID=8DA8504A71DF0BAA50DA29D0AFAD6862; PSTM=1568099632; MCITY=-315%3A; delPer=0; PSINO=5; BK_SEARCHLOG=%7B%22key%22%3A%5B%22%E7%8E%8B%E5%B2%90%E5%B1%B1%22%2C%22%E6%B8%A9%E5%AE%B6%E5%AE%9D%22%2C%22%E6%B1%9F%E6%B3%BD%E6%B0%91%22%2C%22%E8%83%A1%E9%94%A6%E6%B6%9B%22%2C%22AC%22%2C%22AK%22%5D%7D; BDRCVFR[dG2JNJb_ajR]=mk3SLVN4HKm; BDRCVFR[-pGxjrCMryR]=mk3SLVN4HKm; BDRCVFR[tox4WRQ4-Km]=mk3SLVN4HKm; H_PS_PSSID=1464_31669_32046_32231_32323_32117_32092; Hm_lvt_55b574651fcae74b0a9f1cf9c8d7c93a=1595223173,1595227193,1595233581,1595233782; Hm_lpvt_55b574651fcae74b0a9f1cf9c8d7c93a=1595233788
Host: baike.baidu.com
Referer: https://www.dogedoge.com/results?q=%E4%B8%AD%E5%9B%BD%E7%A5%A8%E6%88%BF%E6%8E%92%E8%A1%8C
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: cross-site
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36
    """
    headers = get_headers(headers)
    url = "https://baike.baidu.com/item/%E4%B8%AD%E5%9B%BD%E7%94%B5%E5%BD%B1%E7%A5%A8%E6%88%BF/4101787"
    response = requests.get(url, headers=headers)
    if not response.status_code == 200:
        print("error...")
        raise ValueError

    html = HTML(response.text)

    records = {}
    movie_names = html.xpath("//td/a[@target='_blank']/text()")
    movie_urls = html.xpath("//td/a[@target='_blank']/@href")
    host = "https://baike.baidu.com"
    for movie_name, movie_url in zip(movie_names, movie_urls):
        to_url = host + movie_url
        movie = records.setdefault(movie_name, {})
        movie["name"] = movie_name
        movie["to_url"] = to_url

        response2 = requests.get(to_url, headers=headers)
        if not response2.status_code == 200:
            print("level 2 error...")
            break
        html2 = HTML(response2.text)
        actors = html2.xpath("//dl[@class='info']/dt/a/text()")
        empolyees = html2.xpath("//td[@class='list-value']/text()")
        titles = html2.xpath("//td[@class='list-key']/text()")
        for title, empolyee in zip(titles, empolyees):
            movie[title] = empolyee.split("、")

        movie["actors"] = actors

    with open("movies.json", "w", encoding="utf-8") as file:
        json.dump(records, file, ensure_ascii=False, indent=1)
