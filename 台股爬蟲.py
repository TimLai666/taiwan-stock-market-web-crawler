import pandas as pd
import requests
from io import StringIO
import time, datetime
import os
import glob

def 儲存csv檔(df, 年份, 月份, 資料種類):
    if df.empty != True:
        if 資料種類 == "每月營收":
                df.to_csv("data/" + str(年份) + "年" + str(月份) + "月營業收入統計.csv")
                print(年份,",",月份)

def 合併csv檔(資料種類):
    if 資料種類 == "每月營收":
        要合併的檔案 = glob.glob("data/*月營業收入統計.csv")
        df = pd.concat(
            map(pd.read_csv, 要合併的檔案), ignore_index = True
        )

        # 去除不需要的東西
        df = df.drop(["Unnamed: 0", "備註"],axis = "columns")

        df.to_csv("每月營業收入統計.csv")

def 取得歷史資料(今年年份, 本月月份):
    # 建立存檔案的資料夾
    if not os.path.exists("data/"):
        os.mkdir("data/")

    print("正在取得每月營收資料...")
    for 年份 in range(2003, 今年年份 + 1):
        if 年份 != 今年年份:
            for 月份 in range(1, 12 + 1):
                df = 當月營收(年份, 月份)
                儲存csv檔(df, 年份, 月份, "每月營收")        
        else:
            for 月份 in range(1, 本月月份):
                df = 當月營收(年份, 月份)
                儲存csv檔(df, 年份, 月份, "每月營收")
        # 偽停頓
        #time.sleep(0.5) 
  
def 當月營收(西元年份, 月份):
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
        
        # 下載該年月的網站，並用pandas轉換成 dataframe
        r = requests.get(url, headers=headers)
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
    else:
        df = pd.DataFrame({})
        df = df.iloc[0:0]

    return df

'''def 個股每日資料(西元年份, 月份, 日期):
    if not os.path.isfile(str(西元年份) + "年" + str(月份) + "月"+ str(日期) +"個股每日資料.csv"):
        if str(月份).len() < 2:
            二位數月份 = "0" + str(月份)
        else:
            二位數月份 = str(月份)

        url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date=" + str(西元年份) + 二位數月份 + str(日期)"&selectType=ALL&response=html"
        
        # 偽瀏覽器
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        
        # 下載該年月的網站，並用pandas轉換成 dataframe
        r = requests.get(url, headers=headers)
        r.encoding = "big5"

        dfs = pd.read_html(StringIO(r.text), encoding = "big5")
        df =
            橫軸索引 = df.index[(df[0] == "證券代號")][0]
            df.columns = df.iloc[橫軸索引]
            df.rename(columns = {"證券代號" : "公司代號"})
        
        df["本益比"] = pd.to_numeric(df["本益比"], "coerce")
        df["殖利率(%)"] = pd.to_numeric(df["殖利率(%)"], "coerce")
        df["股價淨值比"] = pd.to_numeric(df["股價淨值比"], "coerce")
        df["年份"] = 西元年份
        df["月份"] = 月份
    else:
        df = pd.DataFrame({})
        df = df.iloc[0:0]

    return df'''

台灣時區 = datetime.timezone(datetime.timedelta(hours = 8))
今年年份 = int(datetime.datetime.now(tz = 台灣時區).strftime("%Y"))
本月月份 = int(datetime.datetime.now(tz = 台灣時區).strftime("%m"))
取得歷史資料(今年年份, 本月月份)
print("正在合併資料...")
合併csv檔("每月營收")
print("資料更新成功")
input("按enter鍵結束程式")