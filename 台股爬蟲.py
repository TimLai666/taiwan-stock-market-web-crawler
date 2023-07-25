import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup
import time, datetime, calendar
import os
import glob
import requests
import re
import random

def 取得ProxyIP():
    已驗證的IP = []
    待驗證的IP = []
    while len(已驗證的IP) < 1:
        print("正在取得可用IP...")
        if os.path.isfile("data/proxy_list.txt"):
            with open("data/proxy_list.txt", 'r') as file:
                待驗證的IP = file.read().splitlines()
            file.close()
    
        if len(待驗證的IP) < 5:
            response = requests.get("https://www.sslproxies.org/")
            response1 = requests.get("https://free-proxy-list.net/")
            response2 = requests.get("https://www.socks-proxy.net/")
            response3 = requests.get("https://free-proxy-list.net/anonymous-proxy.html")
        
            待驗證的IP += re.findall('\d+\.\d+\.\d+\.\d+:\d+', response.text)  #「\d+」代表數字一個位數以上
            待驗證的IP += re.findall('\d+\.\d+\.\d+\.\d+:\d+', response1.text)
            待驗證的IP += re.findall('\d+\.\d+\.\d+\.\d+:\d+', response2.text)
            待驗證的IP += re.findall('\d+\.\d+\.\d+\.\d+:\d+', response3.text)

            待驗證的IP = list(set(待驗證的IP)) # 清除重複的ip

        print("正在驗證IP...")
        for ip in 待驗證的IP:
            print(ip, end = "--")
            try:
                result = requests.get(
                    'https://www.twse.com.tw/zh/index.html',proxies={'http': ip, 'https': ip},timeout= 3
                    )
                已驗證的IP.append(ip)
                print("有效")
                
                # 把已驗證的ip存到檔案
                with open("data/proxy_list.txt", 'w') as file:
                    for ip in 已驗證的IP:
                        file.write(ip + '\n')
                file.close()
            except:
                print("無效")
    
    return 已驗證的IP

def 重新取得IP():
    print("正在重新取得IP...")
    with open("data/proxy_list.txt", 'w') as file:
        file.write("")
    file.close()
    新IP = 取得ProxyIP()
    return 新IP

def 儲存csv檔(df, 年份, 月份, 日期, 資料種類):
    if df.empty != True:
        if 資料種類 == "每月營收":
            df.to_csv("data/" + str(年份) + "年" + str(月份) + "月營業收入統計.csv")
            print(年份, ",", 月份)
        elif 資料種類 == "個股每日資料":
            df.to_csv("data/" + str(年份) + "年" + str(月份) + "月"+ str(日期) + "日個股資料.csv")
            print(年份, ",", 月份, ",", 日期)

def 合併csv檔(資料種類):
    if 資料種類 == "每月營收":
        要合併的檔案 = glob.glob("data/*月營業收入統計.csv")
        df = pd.concat(
            map(pd.read_csv, 要合併的檔案), ignore_index = True
        )

        # 去除不需要的東西
        df = df.drop(["Unnamed: 0", "備註"],axis = "columns")

        df.to_csv("每月營業收入統計.csv")

    if 資料種類 == "個股每日資料":
        要合併的檔案 = glob.glob("data/*日個股資料.csv")
        df = pd.concat(
            map(pd.read_csv, 要合併的檔案), ignore_index = True
        )
        
        df.to_csv("個股每日資料統計.csv")

def 取得歷史資料(今年年份, 本月月份, 今天日期, ProxyIP):
    # 建立存檔案的資料夾
    if not os.path.exists("data/"):
        os.mkdir("data/")

    print("正在取得每月營收資料...")
    for 年份 in range(2003, 今年年份 + 1):
        if 年份 != 今年年份:
            for 月份 in range(1, 12 + 1):
                df = 當月營收(年份, 月份, ProxyIP)
                儲存csv檔(df, 年份, 月份, 0, "每月營收")        
        else:
            for 月份 in range(1, 本月月份):
                df = 當月營收(年份, 月份, ProxyIP)
                儲存csv檔(df, 年份, 月份, 0, "每月營收")
        
    print("正在取得個股每日資料...")
    for 年份 in range(2006, 今年年份 + 1):
        if 年份 != 今年年份:
            for 月份 in range(1, 12 + 1):
                _, 當月天數 = calendar.monthrange(年份, 月份)
                for 日期 in range (1, 當月天數 + 1):
                    df = 個股當日資料(年份, 月份, 日期, ProxyIP)
                    儲存csv檔(df, 年份, 月份, 日期, "個股每日資料")
        else:
            for 月份 in range(1, 本月月份 + 1):
                _, 當月天數 = calendar.monthrange(年份, 月份)
                for 日期 in range (1, 今天日期):
                    df = 個股當日資料(年份, 月份, 日期, ProxyIP)
                    儲存csv檔(df, 年份, 月份, 日期, "個股每日資料")
  
def 當月營收(西元年份, 月份, ProxyIP):
    if not os.path.isfile("data/" + str(西元年份) + "年" + str(月份) + "月營業收入統計.csv"):
        # 假如是西元，轉成民國
        if 西元年份 > 1990:
            民國年份 = 西元年份 - 1911
        
        if 民國年份 <= 98:
            url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(民國年份)+'_'+str(月份)+'.html'
        else:
            url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(民國年份)+'_'+str(月份)+'_0.html'
        
        # 偽瀏覽器
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        
        n=0
        proxy_ip = ProxyIP[n]

        # 下載該年月的網站，並用pandas轉換成 dataframe
        while True:
            try:
                time.sleep(random.uniform(0.1, 7))
                r = requests.get(
                    url, headers = headers, proxies = {'http': f'{proxy_ip}', 'https': f'{proxy_ip}'}
                    )
                break
            except:
                if n < len(ProxyIP) - 1:
                    n += 1
                    proxy_ip = ProxyIP[n]
                else:
                    ProxyIP = 重新取得IP()
                    n = 0
                    proxy_ip = ProxyIP[n]

        r.encoding = "big5"

        dfs = pd.read_html(StringIO(r.text), encoding = "big5")

        df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
        
        if "levels" in dir(df.columns):
            df.columns = df.columns.get_level_values(1)
        else:
            df = df[list(range(0,10))]
            橫軸索引 = df.index[(df[0] == "公司代號")][0]
            df.columns = df.iloc[橫軸索引]
        
        df["當月營收"] = pd.to_numeric(df["當月營收"], "coerce")
        df = df[~df["當月營收"].isnull()]
        df = df[df["公司代號"] != "合計"]
        df["年份"] = 西元年份
        df["月份"] = 月份
        df["年月"] = str(西元年份) + "/" + str(月份)
    else:
        df = pd.DataFrame({})
        df = df.iloc[0:0]

    return df

def 個股當日資料(西元年份, 月份, 日期, ProxyIP):
    if not os.path.isfile("data/" + str(西元年份) + "年" + str(月份) + "月"+ str(日期) +"日個股資料.csv"):
        if len(str(月份)) < 2:
            二位數月份 = "0" + str(月份)
        else:
            二位數月份 = str(月份)

        url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date=" + str(西元年份) + 二位數月份 + str(日期) + "&selectType=ALL&response=html"
        
        # 偽瀏覽器
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        
        n=0
        proxy_ip = ProxyIP[n]

        # 下載該年月的網站，並用pandas轉換成 dataframe
        while True:
            try:
             time.sleep(random.uniform(0.1, 7))
             r = requests.get(
                 url, headers = headers, proxies = {'http': f'{proxy_ip}', 'https': f'{proxy_ip}'}
                 )
             break
            except:
                if n < len(ProxyIP) - 1:
                    n += 1
                    proxy_ip = ProxyIP[n]
                else:
                    ProxyIP = 重新取得IP()
                    n = 0
                    proxy_ip = ProxyIP[n]

        r.encoding = "utf-8"

        # 檢查有沒有表格
        soup = BeautifulSoup(r.text, 'html.parser')
        表格 = soup.find_all('table')
        if 表格:
            dfs = pd.read_html(StringIO(r.text), encoding = "utf-8")
            df = dfs[0]
            df.columns = df.columns.droplevel(0)
            df.rename(columns = {"證券代號" : "公司代號", "證券名稱":"公司名稱"}, inplace=True)
            df.set_index('公司代號', inplace=True)

            #橫軸索引 = df.index[(df[0] == "證券代號")][0]
            #df.columns = df.iloc[橫軸索引]
            
            df["本益比"] = pd.to_numeric(df["本益比"], "coerce")
            df["殖利率(%)"] = pd.to_numeric(df["殖利率(%)"], "coerce")
            df["股價淨值比"] = pd.to_numeric(df["股價淨值比"], "coerce")
            df["年份"] = 西元年份
            df["月份"] = 月份
            df["日期"] = 日期
            df["年月日"] = str(西元年份) + "/" + str(月份) + "/" + str(日期)
        else:
            df = pd.DataFrame({})
            df = df.iloc[0:0]
    else:
        df = pd.DataFrame({})
        df = df.iloc[0:0]

    return df

台灣時區 = datetime.timezone(datetime.timedelta(hours = 8))
今年年份 = int(datetime.datetime.now(tz = 台灣時區).strftime("%Y"))
本月月份 = int(datetime.datetime.now(tz = 台灣時區).strftime("%m"))
今天日期 = int(datetime.datetime.now(tz = 台灣時區).strftime("%d"))

ProxyIP = 取得ProxyIP()
取得歷史資料(今年年份, 本月月份, 今天日期, ProxyIP)
print("正在合併資料...")
合併csv檔("每月營收")
合併csv檔("個股每日資料")
print("資料更新成功")
input("按enter鍵結束程式")