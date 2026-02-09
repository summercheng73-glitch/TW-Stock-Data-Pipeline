import yfinance as yf #Yahoo Finance API (抓股票資料)資料來源
import pandas as pd #資料整理核心 (DataFrame 二維資料)
import pyodbc #連接SOL Server (ODBC=Open Database Connectivity)
from datetime import datetime #處理時間 SQL的date需要py datetime物件對應

stock_symbol = '2330.TW' #抓台積電 股票代號.TW 將代號存成變數
start_date = '2024-01-01'#抓取2024到至今的資料 yfinance 內部自動轉成datetime
server = "DESKTOP-AJJRVG2\\SQLSERVER2022"#\\是py字串中代表一個\
database = 'FinanceDB' #指定連線的資料庫名稱

print (f"正在下載{stock_symbol} 的資料...")#f-string字串插值語法
try: #try/except 防止API失敗 程式直接崩潰(穩定性設計)
    df = yf.download(stock_symbol, start=start_date)
    #使用yf下載股票資料 回傳型態:pandas DataFrame  index:Date(交易日)
    
    if df.empty:#檢查是否有下載到資料
        print("下載失敗")#失敗印出
        exit() #如沒有則終止
    print(f"下載成功，共得到{len(df)} 筆交易資料")
    #成功則印出(len(df)交易日數量)

except Exception as e :#補捉下載過程中的任何意外 Exception所有錯誤的父類別
    print(f"下載失敗{e}")#e會包含實際錯誤原因
    exit() #失敗中止

if isinstance(df.columns, pd.MultiIndex):#isinstance 檢查物件
    #檢查是否為MultiIndex(多層欄位) 寫入SQL前必須轉成單層欄位
    df.columns= df.columns.get_level_values(0)
    #若是 只保留第一層欄位 ex:('Open','2330.TW')>'Open'

df.reset_index(inplace=True) 
#把Date從index變成一般欄位 (index是DataFrame的行標籤從0開始)
df=df.fillna(0) #將NaN補成0(Data Cleaning的一部份)

conn_str=f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
#建立ODBC連線字串 Trusted_Connection=yes 使用Windows驗證
print("正在連線資料庫") #印出

try:#嘗試建立資料庫連線
    conn=pyodbc.connect(conn_str) #conn代表一條資料庫連線
    cursor=conn.cursor() #建立游標 Cursor用來執行SQL指令
    success_count=0 #用來統計寫入的資料量

    for index, row in df.iterrows():#index列編號 row該列的資料
        try:#每一筆都建立獨立的try 避免單筆影響全部
            date_val=row['Date'].to_pydatetime().date() 
            #將pandas Timestamp轉成python date
            open_val=float(row['Open'])
            high_val=float(row['High'])
            low_val=float(row['Low'])
            close_val=float(row['Close'])
            vol_val=int(row['Volume'])
            #確保SQL欄位相容 將DataFrame欄位轉成python型態

            #開始定義SQL(多行字串)
            sql_query = """
            IF NOT EXISTS (SELECT 1 FROM Stock_Data WHERE StockSymbol = ? AND TradeDate = ?)
            BEGIN
                INSERT INTO Stock_Data (StockSymbol, TradeDate, OpenPrice, HightPrice, LowPrice, ClosePrice, Volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            END
            """ 
            # ? 是參數占位符 只有不存在才插入(防止SQL Injection)

            params = (stock_symbol, date_val, 
                      stock_symbol, date_val, open_val, high_val, low_val, close_val, vol_val)
            #實際傳入SQL的參數值(順序必須完全對應)由上到下 由左到右 有9個?

            cursor.execute(sql_query, params) #執行SQL指令(尚未寫入磁碟)
            success_count += 1 #成功寫入一筆資料 計數+1
            
        except Exception as inner_e:#捕捉單筆資料錯誤
            print(f"處理第 {index} 筆資料時發生錯誤: {inner_e}")
            continue #顯示錯誤並跳過該筆資料

    conn.commit() #提交交易 (真正寫入SQL) 沒有 commit資料不會永久儲存
    print(f"處理完成 已嘗試處理{success_count} 筆資料")
    #顯示總處理筆數
    
except pyodbc.Error as ex:#捕捉資料庫錯誤
    print(f"錯誤{ex}") #顯示SQL回傳的錯誤訊息
    if 'conn' in locals(): #若發生錯誤 資料不寫入
        conn.rollback() #回去交易
finally:#無論成功或失敗都會執行
    if 'conn' in locals(): 
        conn.close() #關閉資料庫連線 防止連線洩漏
        print("資料庫已關閉")
