# Airflow + Mongodb --- TW_Stock

[Github連結]()

# Objective:
- 希望能抓取每日台股資料存到metadata的collection(table)，做完簡單的資料處理後，再存到cleandata的collection。
    - 為了練習Mongodb的語法，在這裡所有資料處理都以MongoHook的語法做(後面會介紹)，但其實也可以直接用python來處理。
    - 為了練習airflow的語法，因此我把dag弄得滿複雜的，所以程式碼也很冗。

- PS: 對我來說算是一次學三個東西 Airflow, Mongodb, WSL，而且過去對工作排程、NO-SQL都沒經驗，所以踩蠻多坑的，ㄏㄏ，另外我才疏學淺，以下內容若有誤，歡迎糾正。


# Requirements:
- MongoServer:
    - Mongodb==4.4
- python:
    - apache-airflow[mongo]
    - pandas
    - pymongo


# MongoHook
![](https://i.imgur.com/mcBmv3B.png)
- Airflow有提供一個叫MongoHook的東西，讓使用者可以設定好Mongodb的位址、帳密...，之後只需要在MongoHook丟conn_id，即可連線。
- MongoHook的程式碼也就是Airflow的BaseHook + pymongo而已。[可以在這裡看到](https://airflow.apache.org/docs/stable/_modules/airflow/contrib/hooks/mongo_hook.html)

## 如何設定?
1. 在webserver的上方找到Admin/Connections，並點擊
    - ![](https://i.imgur.com/8L8kXjo.png)
2. 在該頁面找到你要用的conn_id(我們這裡是"mongo_default")，並點擊左邊鉛筆的圖案。(也可以自行建立一個conn_id)
    - ![](https://i.imgur.com/tPR2t6e.png)
3. 到這頁面就可以自己改啦~~~
    - ![](https://i.imgur.com/tjVwr7A.png)

## 如何使用?
- 之前說過了，MongoHook就是包起來的pymongo而已，因此其query跟pymongo就是一模一樣，詳情得自己看文件歐~~~
- [MongoHook文件連結(就insert、update...那些的啦)](https://airflow.apache.org/docs/stable/_modules/airflow/contrib/hooks/mongo_hook.html)
- [Mongodb文件連結](https://docs.mongodb.com/manual/)
- ![](https://i.imgur.com/8MkYuni.png)


# 流程

## 文字敘述
1. 先判斷今天是否是工作日，若是才接下去跑。
1. 抓台股資料(得到pd.DataFrame)
    - ![](https://i.imgur.com/ZujLHgi.png)
2. 存到metadata collection (list of dict)
    - ![](https://i.imgur.com/vEEszyD.png)
3. 從metadata篩選出今天的資料，先存到tmpYYYYmmdd的collection
4. 做資料清理
    1. 先將各個columns改名
    2. 保留之後要用的columns(砍掉沒要用到的)
    3. 將各個columns轉成數字(srt -> double)
    4. 把 漲跌(+/-) 跟 漲跌價差，合起來變成漲跌
5. 最後將其tmp的資料insert到cleandata collection，並清空tmp
    - ![](https://i.imgur.com/7E31O4j.png)


## Dag
- TW_Stock，其包含兩個subdag: get_metadata, clean_data
![](https://i.imgur.com/KIaePQs.png)

- get_metadata:
![](https://i.imgur.com/gUebZuC.png)

- clean_data
![](https://i.imgur.com/pl8mu3Q.png)

