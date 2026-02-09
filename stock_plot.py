import pyodbc #py連線SQL
import pandas as pd #資料分析與表格處理核心
import matplotlib.pyplot as plt #python最基礎穩定的繪圖函式庫
import matplotlib.dates as mdates #matplotlib的日期處理器

server = "DESKTOP-AJJRVG2\\SQLSERVER2022"
database = 'FinanceDB'
stock_symbol = '2330.TW' #將股票參數化 方便未來換股票

conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

print("正從SQL 讀取結果")


sql_query="""
select
    TradeDate,
    ClosePrice,
    avg(ClosePrice) over (order by TradeDate rows between 4 preceding and current row) as MA5,
    avg(ClosePrice) over (order by TradeDate rows between 19 preceding and current row) as MA20,
    (ClosePrice - lag(ClosePrice) over (order by TradeDate)) / lag(ClosePrice) over (order by TradeDate) * 100 as DailyReturn
from Stock_Data
where StockSymbol = ?
order by TradeDate ASC
"""
try:
    conn=pyodbc.connect(conn_str)
    df = pd.read_sql(sql_query, conn, params=[stock_symbol])
    #執行SQL並直接轉成pandas DataFrame read_sql會自動處理cursor與欄位名稱
    df['TradeDate'] = pd.to_datetime(df['TradeDate'])
    #確保TradeDate是datetime型態 
    conn.close()
    print(f"成功讀取{len(df)}筆資料")

except Exception as e :
    print(f"錯誤:{e}")
    exit()

plt.figure(figsize=(12, 8))
#建立圖表畫布 設定大小(寬12 高8)

plt.subplot(2, 1, 1)
#建立2列1欄的第一個子圖
plt.plot(df['TradeDate'], df['ClosePrice'], label='Close Price', color= 'black', alpha=0.6)
#alpha:透明度
plt.plot(df['TradeDate'], df['MA5'], label='MA 5(week)', color= 'orange', linewidth=1.5)
plt.plot(df['TradeDate'], df['MA20'], label='MA 20(Month)', color= 'blue', linewidth=1.5)

plt.title(f'{stock_symbol} Price Trend & Moving Averages', fontsize=14, fontweight='bold')
#設定標題
plt.ylabel('Price (TWD)')
#設定Y軸標籤
plt.legend() #顯示圖例
plt.grid(True, linestyle='--', alpha=0.5)
#顯示格線 提高可讀性

plt.subplot(2, 1, 2) #第二個子圖
colors=['red' if x > 0 else 'green' for x in df['DailyReturn']]
#依報酬正負決定長條圖顏色
plt.bar(df['TradeDate'], df['DailyReturn'],color=colors, alpha=0.8)
#劃出每日報酬率
plt.title('Daily Return % (Volatility)', fontsize=12)
plt.ylabel('Return %')
plt.axhline(0, color='black', linewidth=0.8)
#劃出0基準線
plt.grid(True, linestyle='--', alpha=0.3)
#顯示表格

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
#設定X軸日期顯示格式(年-月)
plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=3))
#設定X軸每3個月顯示一次
plt.gcf().autofmt_xdate()
#自動旋轉日期 避免重疊

plt.tight_layout()
#自動調整子圖間距
plt.savefig('stock_analysis_result.png')
#將圖表儲存圖片檔
print("圖表已儲存")
plt.show()
#顯示圖表視窗