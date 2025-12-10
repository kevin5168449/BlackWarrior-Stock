import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import time
import twstock
import os
import requests
import feedparser
from collections import Counter
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# ★★★ 修正：已移除 FinMind 引用，現在是純爬蟲模式 ★★★

# ==========================================
# 0. 系統設定
# ==========================================
try:
    st.set_page_config(page_title="黑武士・全能戰情室", layout="wide", page_icon="⚔️")
except: pass

HISTORY_FILE = "screening_history.csv"

# 白名單
VALID_STRATEGIES = [
    "籌碼衝鋒 (集中度高)", 
    "蜻蜓點水 (縮量回測)", 
    "浴火重生 (假跌破)"
]

# ==========================================
# 1. 檔案與清洗 / 工具函式
# ==========================================

def get_taiwan_time():
    return datetime.utcnow() + timedelta(hours=8)

def send_line_notify(token, message):
    url = "https://notify-api.line.me/api/notify"
    headers = {"Authorization": f"Bearer {token}"}
    data = {"message": message}
    try:
        requests.post(url, headers=headers, data=data, timeout=5)
    except: pass

def clean_invalid_data():
    if os.path.exists(HISTORY_FILE):
        try:
            df = pd.read_csv(HISTORY_FILE)
            if '策略' in df.columns:
                df_clean = df[df['策略'].isin(VALID_STRATEGIES)]
                if len(df_clean) < len(df):
                    df_clean.to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')
        except: pass

def save_to_history(new_results):
    if not new_results: return
    df_new = pd.DataFrame(new_results)
    current_date = get_taiwan_time().strftime("%Y-%m-%d")
    df_new.insert(0, "篩選日期", current_date)
    if "進場價" not in df_new.columns and "收盤" in df_new.columns:
        df_new["進場價"] = df_new["收盤"]
    
    if os.path.exists(HISTORY_FILE):
        df_old = pd.read_csv(HISTORY_FILE)
        for col in df_new.columns:
             if col not in df_old.columns: df_old[col] = "N/A"
        for col in df_old.columns:
             if col not in df_new.columns: df_new[col] = "N/A"
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_combined = df_combined[df_combined['策略'].isin(VALID_STRATEGIES)]
        df_combined.drop_duplicates(subset=['篩選日期', '代號', '策略'], keep='last', inplace=True)
    else:
        df_combined = df_new
    
    df_combined.to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')
    st.toast(f"✅ 紀錄已儲存 (含營收與籌碼)")

def load_history():
    if os.path.exists(HISTORY_FILE): 
        df = pd.read_csv(HISTORY_FILE)
        if '產業' not in df.columns: df['產業'] = '其他'
        if '營收年增(%)' not in df.columns: df['營收年增(%)'] = "N/A"
        return df[df['策略'].isin(VALID_STRATEGIES)]
    return None

def clear_history():
    if os.path.exists(HISTORY_FILE): os.remove(HISTORY_FILE)

clean_invalid_data()

# ==========================================
# 2. 數據獲取 (核心函數 - 優先加載)
# ==========================================

@st.cache_data(ttl=86400)
def get_tw_stock_list():
    try:
        codes = twstock.codes
        tw_list = []
        for code in codes:
            if codes[code].type == "股票":
                suffix = ".TW" if codes[code].market == "上市" else ".TWO"
                tw_list.append(f"{code}{suffix}")
        return tw_list
    except: return []

@st.cache_data(ttl=86400)
def get_stock_name(code):
    try: return twstock.codes[code].name
    except: return code

SUB_SECTOR_MAP = {
    '2408': '記憶體', '2344': '記憶體', '2337': '記憶體', '3260': '記憶體', '8299': '記憶體',
    '3006': '記憶體', '2451': '記憶體', '4967': '記憶體', '5289': '記憶體',
    '2382': 'AI伺服器', '3231': 'AI伺服器', '2356': 'AI伺服器', '6669': 'AI伺服器', 
    '2317': 'AI伺服器', '2301': 'AI伺服器', '2376': 'AI伺服器',
    '3017': '散熱', '3324': '散熱', '3338': '散熱', '3653': '散熱', '2421': '散熱',
    '2454': 'IC設計', '3034': 'IC設計', '2379': 'IC設計', '3035': 'IC設計', 
    '3529': 'IC設計', '3443': 'IC設計', '8016': 'IC設計', '6415': 'IC設計',
    '1513': '重電綠能', '1519': '重電綠能', '1503': '重電綠能', '1504': '重電綠能',
    '1609': '重電綠能', '6806': '重電綠能',
    '2603': '貨櫃航運', '2609': '貨櫃航運', '2615': '貨櫃航運', 
    '2618': '航空', '2610': '航空', '2637': '散裝航運', '2606': '散裝航運',
    '2330': '晶圓代工', '2303': '晶圓代工', '5347': '晶圓代工'
}

def get_stock_sector(code):
    if code in SUB_SECTOR_MAP: return SUB_SECTOR_MAP[code]
    try: return twstock.codes[code].group
    except: return "其他"

def get_last_trading_day(date_obj):
    offset = 1
    while True:
        prev = date_obj - timedelta(days=offset)
        if prev.weekday() < 5: return prev
        offset += 1

def get_market_temperature():
    try:
        tickers = ['^TWII', '^VIX']
        data = yf.download(tickers, period='5d', progress=False)['Close']
        if not data.empty:
            twii_curr = data['^TWII'].iloc[-1]
            twii_prev = data['^TWII'].iloc[-2]
            twii_pct = ((twii_curr - twii_prev) / twii_prev) * 100
            vix_curr = data['^VIX'].iloc[-1]
            vix_change = vix_curr - data['^VIX'].iloc[-2]
            return {
                'twii': f"{int(twii_curr):,}",
                'twii_change': f"{(twii_curr - twii_prev):+.2f} ({twii_pct:+.2f}%)",
                'vix': f"{vix_curr:.2f}",
                'vix_change': f"{vix_change:+.2f}"
            }
    except: return None
    return None

def calculate_rsi(data, window=14):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def fetch_raw_data(ticker, period="1y"):
    ticker = ticker.strip().upper()
    if not (ticker.endswith(".TW") or ticker.endswith(".TWO")): ticker = f"{ticker}.TW"
    try:
        data = yf.Ticker(ticker).history(period=period)
        if len(data) > 20: 
            data.index = data.index.tz_localize(None)
            return data
    except: pass
    return None

def add_technical_indicators(data_df):
    try:
        data_df['MA5'] = data_df['Close'].rolling(window=5).mean()
        data_df['MA20'] = data_df['Close'].rolling(window=20).mean()
        data_df['MA60'] = data_df['Close'].rolling(window=60).mean()
        data_df['MA200'] = data_df['Close'].rolling(window=200).mean()
        data_df['Volume_MA5'] = data_df['Volume'].rolling(window=5).mean()
        data_df['Volume_MA60'] = data_df['Volume'].rolling(window=60).mean()
        data_df['RSI'] = calculate_rsi(data_df)
        return data_df
    except: return None

def get_stock_fundamentals_safe(ticker):
    try:
        if not ticker.endswith('.TW') and not ticker.endswith('.TWO'): ticker += '.TW'
        stock = yf.Ticker(ticker)
        info = stock.info
        eps = info.get('trailingEps', None)
        pe = info.get('trailingPE', None)
        roe = info.get('returnOnEquity', None)
        return eps, pe, roe
    except: return None, None, None

# ★★★ 關鍵修正：加入偽裝 Headers ★★★
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# --- 營收 (MOPS) ---
@st.cache_data(ttl=3600)
def get_revenue_data_snapshot():
    date_obj = get_taiwan_time()
    if date_obj.day < 12: 
        target_month = date_obj.replace(day=1) - timedelta(days=1)
        target_month = target_month.replace(day=1) - timedelta(days=1)
    else:
        target_month = date_obj.replace(day=1) - timedelta(days=1)
        
    for _ in range(2): 
        roc_year = target_month.year - 1911
        month = target_month.month
        revenue_map = {}
        urls = [
            f"https://mops.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{month}_0.html",
            f"https://mops.twse.com.tw/nas/t21/otc/t21sc03_{roc_year}_{month}_0.html" 
        ]
        has_data = False
        for url in urls:
            try:
                # ★ 加入 Headers + verify=False
                res = requests.get(url, headers=HEADERS, timeout=3, verify=False)
                res.encoding = 'utf-8'
                dfs = pd.read_html(res.text)
                for df in dfs:
                    if df.shape[1] > 5 and '公司代號' in str(df.columns):
                        df.columns = [str(c).replace(' ','') for c in df.columns] 
                        col_code = None
                        col_yoy = None
                        col_mom = None
                        for i, col in enumerate(df.columns):
                            if '代號' in col: col_code = col
                            if '去年' in col and '%' in col: col_yoy = col
                            if '上月' in col and '%' in col: col_mom = col
                        if col_code and col_yoy:
                            for _, row in df.iterrows():
                                try:
                                    code = str(row[col_code])
                                    if code == 'nan' or code == '合計': continue
                                    yoy = float(str(row[col_yoy]).replace(',',''))
                                    mom = float(str(row[col_mom]).replace(',','')) if col_mom else 0.0
                                    revenue_map[code] = {'yoy': yoy, 'mom': mom}
                                    has_data = True
                                except: continue
            except: pass
        if has_data: return revenue_map, f"{roc_year}/{month}"
        target_month = target_month.replace(day=1) - timedelta(days=1)
    return {}, "無資料 (連線逾時)"

# --- 融資 (TWSE + TPEx) ---
@st.cache_data(ttl=3600)
def get_tpex_margin_data_snapshot(date_obj):
    roc_year = int(date_obj.strftime('%Y')) - 1911
    date_str = f"{roc_year}/{date_obj.strftime('%m/%d')}"
    url = f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d={date_str}&s=0,asc,0"
    try:
        res = requests.get(url, headers=HEADERS, timeout=5)
        data = res.json()
        if 'aaData' in data:
            margin_dict = {}
            for row in data['aaData']:
                try:
                    code = row[0]
                    today_bal = int(row[6].replace(',', ''))
                    yest_bal = int(row[2].replace(',', ''))
                    net_change = (today_bal - yest_bal) / 1000 
                    margin_dict[code] = net_change
                except: continue
            return margin_dict
    except: pass
    return {}

@st.cache_data(ttl=3600)
def get_margin_data_snapshot():
    date_obj = get_taiwan_time()
    if date_obj.hour < 21: date_obj -= timedelta(days=1)
    for _ in range(3):
        if date_obj.weekday() >= 5: 
            date_obj -= timedelta(days=1); continue
        date_str = date_obj.strftime('%Y%m%d')
        
        twse_dict = {}
        try:
            url = f"https://www.twse.com.tw/rwd/zh/margin/MI_MARGN?date={date_str}&selectType=STOCK&response=json"
            res = requests.get(url, headers=HEADERS, timeout=5)
            data = res.json()
            if data['stat'] == 'OK':
                for table in data.get('tables', []):
                    if '股票代號' in table['fields'] and '融資今日餘額' in table['fields']:
                        df = pd.DataFrame(table['data'], columns=table['fields'])
                        for col in ['融資前日餘額', '融資今日餘額']:
                             df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                        df['net_change'] = (df['融資今日餘額'] - df['融資前日餘額']) / 1000
                        twse_dict = df.set_index('股票代號')['net_change'].to_dict()
                        break
        except: pass

        tpex_dict = get_tpex_margin_data_snapshot(date_obj)
        if twse_dict or tpex_dict:
            twse_dict.update(tpex_dict)
            return twse_dict
            
        date_obj -= timedelta(days=1)
    return {}

# --- 籌碼 (TWSE + TPEx) ---
@st.cache_data(ttl=3600)
def get_tpex_chip_data_snapshot(date_obj):
    roc_year = int(date_obj.strftime('%Y')) - 1911
    date_str = f"{roc_year}/{date_obj.strftime('%m/%d')}"
    url = f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=json&se=EW&t=D&d={date_str}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=5)
        data = res.json()
        if 'aaData' in data:
            chip_dict = {}
            for row in data['aaData']:
                code = row[0]
                try:
                    net_buy = int(row[-1].replace(',', '')) 
                    chip_dict[code] = net_buy
                except: continue
            return chip_dict
    except: pass
    return {}

@st.cache_data(ttl=3600)
def get_chip_data_snapshot():
    date_obj = get_taiwan_time()
    if date_obj.hour < 15: date_obj -= timedelta(days=1)
    for _ in range(3):
        if date_obj.weekday() >= 5:
            date_obj -= timedelta(days=1); continue
        date_str_twse = date_obj.strftime('%Y%m%d')
        
        twse_dict = {}
        try:
            url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str_twse}&selectType=ALL&response=json"
            res = requests.get(url, headers=HEADERS, timeout=5)
            data = res.json()
            if data['stat'] == 'OK':
                df = pd.DataFrame(data['data'], columns=data['fields'])
                df['三大法人買賣超股數'] = pd.to_numeric(df['三大法人買賣超股數'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                twse_dict = df.set_index('證券代號')['三大法人買賣超股數'].to_dict()
        except: pass

        tpex_dict = get_tpex_chip_data_snapshot(date_obj)
        if twse_dict or tpex_dict:
            twse_dict.update(tpex_dict)
            return twse_dict, date_str_twse
            
        date_obj -= timedelta(days=1)
    return {}, "無資料"

def calculate_chip_concentration_pct(stock_id, chip_map, current_volume):
    net_buy_shares = chip_map.get(stock_id, 0)
    if not chip_map: return 0.0 
    if net_buy_shares <= 0 or current_volume <= 0: return 0.0
    return (net_buy_shares / current_volume) * 100.0

@st.cache_data(ttl=600)
def get_tw_market_heatmap_data():
    date_obj = get_taiwan_time()
    if date_obj.hour < 14: date_obj -= timedelta(days=1)
    for _ in range(5):
        if date_obj.weekday() >= 5: 
            date_obj -= timedelta(days=1); continue
        date_str = date_obj.strftime('%Y%m%d')
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date_str}&type=ALLBUT0999&response=json"
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            data = res.json()
            if data['stat'] == 'OK':
                target_table = None
                for table in data.get('tables', []):
                    if '證券代號' in table['fields'] and '收盤價' in table['fields']:
                        target_table = table; break
                if target_table:
                    df = pd.DataFrame(target_table['data'], columns=target_table['fields'])
                    df['成交金額'] = pd.to_numeric(df['成交金額'].astype(str).str.replace(',', '').replace('--', '0'), errors='coerce').fillna(0)
                    df['收盤價'] = pd.to_numeric(df['收盤價'].astype(str).str.replace(',', '').replace('--', '0'), errors='coerce').fillna(0)
                    df['漲跌價差'] = pd.to_numeric(df['漲跌價差'].astype(str).str.replace(',', '').replace('--', '0'), errors='coerce').fillna(0)
                    def parse_sign(txt): return 1 if '+' in txt else (-1 if '-' in txt else 0)
                    df['sign'] = df['漲跌(+/-)'].astype(str).apply(parse_sign)
                    df['漲跌金額'] = df['漲跌價差'] * df['sign']
                    df['昨日收盤'] = df['收盤價'] - df['漲跌金額']
                    df['漲跌幅%'] = 0.0
                    mask = df['昨日收盤'] > 0
                    df.loc[mask, '漲跌幅%'] = (df.loc[mask, '漲跌金額'] / df.loc[mask, '昨日收盤']) * 100
                    df['漲跌幅%'] = df['漲跌幅%'].round(2)
                    df_top = df.sort_values('成交金額', ascending=False).head(400).copy() 
                    def get_sector_enhanced(code):
                        if code in SUB_SECTOR_MAP: return SUB_SECTOR_MAP[code]
                        try: return twstock.codes[code].group
                        except: return "其他"
                    df_top['產業'] = df_top['證券代號'].apply(get_sector_enhanced)
                    df_top['標籤'] = df_top['證券名稱'] + "<br>" + df_top['漲跌幅%'].astype(str) + "%"
                    return df_top, date_str
        except: pass
        date_obj -= timedelta(days=1)
    return None, "無資料"

@st.cache_data(ttl=1800)
def get_all_market_news():
    rss_sources = {
        "Yahoo個股": "https://tw.stock.yahoo.com/rss?category=tw-individual",
        "Yahoo產業": "https://tw.stock.yahoo.com/rss?category=tw-industry",
        "MoneyDJ個股": "https://www.moneydj.com/KMDJ/RssCenter.aspx?svc=NW&a=X0100000",
        "MoneyDJ產業": "https://www.moneydj.com/KMDJ/RssCenter.aspx?svc=NW&a=X0200000"
    }
    all_news = []
    seen_titles = set()
    keywords = []
    for source, url in rss_sources.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:8]: 
                if entry.title not in seen_titles:
                    all_news.append({"來源": source, "標題": entry.title, "連結": entry.link, "時間": entry.get('published', '')})
                    seen_titles.add(entry.title)
                    if "營收" in entry.title: keywords.append("營收")
                    if "法說" in entry.title: keywords.append("法說")
                    if "新高" in entry.title: keywords.append("創新高")
        except: pass
    return all_news, keywords

@st.cache_data(ttl=600)
def get_twse_sector_flow_dynamic():
    url_base = "https://www.twse.com.tw/rwd/zh/afterTrading/BFIAMU?response=json"
    try:
        res = requests.get(url_base, timeout=10)
        data = res.json()
        if data.get('stat') != 'OK': return None, "無資料", None, None
        df_curr = pd.DataFrame(data['data'], columns=data['fields'])
        df_curr['成交金額'] = pd.to_numeric(df_curr['成交金額'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        total_curr = df_curr['成交金額'].sum()
        df_curr['今日佔比%'] = (df_curr['成交金額'] / total_curr) * 100 if total_curr > 0 else 0
        if df_curr['漲跌指數'].astype(str).str.contains('<').any():
             df_curr['漲跌指數'] = df_curr['漲跌指數'].astype(str).str.extract(r'>([-\d\.]+)<')[0]
        today = datetime.strptime(data['date'], '%Y%m%d')
        prev_str = get_last_trading_day(today).strftime('%Y%m%d')
        try:
            res_p = requests.get(f"{url_base}&date={prev_str}", timeout=5)
            data_p = res_p.json()
            if data_p.get('stat') == 'OK':
                df_p = pd.DataFrame(data_p['data'], columns=data_p['fields'])
                df_p['成交金額'] = pd.to_numeric(df_p['成交金額'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                total_p = df_p['成交金額'].sum()
                df_p['昨日佔比%'] = (df_p['成交金額'] / total_p) * 100 if total_p > 0 else 0
                df_merge = pd.merge(df_curr, df_p[['分類指數名稱', '昨日佔比%']], on='分類指數名稱', how='left')
                df_merge['資金變動%'] = df_merge['今日佔比%'] - df_merge['昨日佔比%'].fillna(0)
            else: df_merge = df_curr; df_merge['資金變動%'] = 0
        except: df_merge = df_curr; df_merge['資金變動%'] = 0
        df_merge = df_merge.round(1)
        flow_in = df_merge.sort_values('資金變動%', ascending=False).head(5)
        flow_out = df_merge.sort_values('資金變動%', ascending=True).head(5)
        main_s = df_merge.sort_values('成交金額', ascending=False).head(10)
        return main_s, flow_in, flow_out, data['date']
    except Exception as e: return None, str(e), None, None

@st.cache_data(ttl=600)
def get_institutional_ranking_smart():
    url = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALL"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        if data.get('stat') != 'OK': return None, "無資料"
        df = pd.DataFrame(data['data'], columns=data['fields'])
        target_col = '三大法人買賣超股數'
        for c in df.columns:
            if '三大法人' in c and '買賣超' in c: target_col = c; break
        df[target_col] = pd.to_numeric(df[target_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        df_today = df[['證券代號', '證券名稱', target_col]].copy()
        df_today.columns = ['代號', '名稱', '今日買超']
        top_list = df_today.sort_values('今日買超', ascending=False).head(30).copy()
        today = datetime.strptime(data['date'], '%Y%m%d')
        prev_str = get_last_trading_day(today).strftime('%Y%m%d')
        try:
            res_p = requests.get(f"https://www.twse.com.tw/rwd/zh/fund/T86?date={prev_str}&response=json&selectType=ALL", timeout=5)
            d_p = res_p.json()
            if d_p.get('stat') == 'OK':
                df_p = pd.DataFrame(d_p['data'], columns=d_p['fields'])
                df_p[target_col] = pd.to_numeric(df_p[target_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
                df_p = df_p[['證券代號', target_col]]
                df_p.columns = ['代號', '昨日買超']
                top_list = pd.merge(top_list, df_p, on='代號', how='left').fillna(0)
            else: top_list['昨日買超'] = 0
        except: top_list['昨日買超'] = 0
        def label(row):
            t, y = row['今日買超'], row['昨日買超']
            if t > 0 and y > 0: return "🚀 爆買" if t > y*2 and t>1000000 else "🔥 連買"
            if t > 0 and y < 0: return "⚡ 強勢轉買" if t > abs(y) else "⚡ 轉買"
            return "💰 大戶進場" if t > 2000000 else "買超"
        top_list['狀態'] = top_list.apply(label, axis=1)
        top_list['今日(張)'] = (top_list['今日買超']/1000).astype(int)
        return top_list[['代號', '名稱', '今日(張)', '狀態']], data['date']
    except Exception as e: return None, str(e)

# ==========================================
# 3. 核心策略
# ==========================================

def is_bullish_candlestick(open_p, close_p, high_p, low_p):
    if close_p > open_p: return True
    total_len = high_p - low_p
    body_len = abs(close_p - open_p)
    if total_len > 0 and (body_len / total_len < 0.1): return True
    if open_p > 0 and (body_len / open_p < 0.003): return True
    lower_shadow = min(open_p, close_p) - low_p
    if total_len > 0 and (lower_shadow / total_len > 0.5): return True
    return False

def check_stock_strategy_web(df, settings, ticker="", chip_map=None):
    if df is None or len(df) < 60: return False
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    strategy = settings['strategy']
    stock_id = ticker.split('.')[0] if ticker else ""
    
    if strategy != '蜻蜓點水 (縮量回測)':
        if pd.isna(curr['MA200']): return False
        bias = ((curr['Close'] - curr['MA200']) / curr['MA200']) * 100
        if strategy != '浴火重生 (假跌破)' and abs(bias) > settings['bias_range']: return False

    if settings['check_trend_high']:
        past_60 = df.iloc[-65:-5]
        if not past_60.empty and not pd.isna(curr['MA200']):
            past_high = past_60['Close'].max()
            if past_high <= (curr['MA200'] * 1.05): return False

    if settings['check_rsi_rising']:
        if pd.isna(curr['RSI']) or curr['RSI'] <= prev['RSI']: return False 
    
    if settings['vol_surge']:
        if curr['Volume'] <= prev['Volume']: return False 
    
    if settings['check_red_candle']:
        if not is_bullish_candlestick(curr['Open'], curr['Close'], curr['High'], curr['Low']): return False

    # 策略邏輯
    if strategy == '籌碼衝鋒 (集中度高)':
        if curr['Close'] <= curr['MA20']: return False
        chip_status = "⚠️ 無籌碼數據"
        if chip_map:
            concentration = calculate_chip_concentration_pct(stock_id, chip_map, curr['Volume'])
            if concentration >= settings.get('chip_threshold', 10.0):
                net_buy = int(chip_map.get(stock_id, 0) / 1000)
                chip_status = f"籌碼集中 {concentration:.1f}% (買超{net_buy}張)"
            else: return False
        return True, chip_status
    
    elif strategy == '蜻蜓點水 (縮量回測)':
        if curr['Close'] < curr['MA200']: return False
        if curr['Low'] > curr['MA200'] * 1.03: return False 
        if pd.isna(curr['Volume_MA5']) or curr['Volume'] > curr['Volume_MA5']: return False 
        net_buy = 0
        if chip_map: net_buy = int(chip_map.get(stock_id, 0) / 1000)
        return True, f"量縮有撐 (買超{net_buy}張)"

    elif strategy == '浴火重生 (假跌破)':
        if curr['Close'] <= curr['MA200']: return False
        past_10 = df.iloc[-11:-1]
        if past_10.empty: return False
        is_break = (past_10['Low'] < past_10['MA200']).any()
        if is_break:
            net_buy = 0
            if chip_map: net_buy = int(chip_map.get(stock_id, 0) / 1000)
            return True, f"假跌破回穩 (買超{net_buy}張)"

    return False

# ==========================================
# 4. 回測核心
# ==========================================
def calculate_forward_performance(df, signal_loc):
    results = {}
    try:
        signal_price = df['Close'].iloc[signal_loc]
        future_data = df.iloc[signal_loc + 1 : ] 
        if not future_data.empty:
            max_price = future_data['High'].max()
            max_idx = future_data['High'].idxmax()
            if signal_price > 0:
                max_gain = ((max_price - signal_price) / signal_price) * 100
            else: max_gain = 0
            results["波段最高漲幅(%)"] = round(max_gain, 2)
            results["最高價日期"] = max_idx.strftime('%Y-%m-%d')
            results["持有天數"] = (max_idx - df.index[signal_loc]).days
        else:
            results["波段最高漲幅(%)"] = 0.0
            results["最高價日期"] = "N/A"
            results["持有天數"] = 0
    except:
        results["波段最高漲幅(%)"] = 0.0
        results["最高價日期"] = "Error"
        results["持有天數"] = 0
    return results

def check_signal_on_date(df, target_date_str, settings, strict_mode=True):
    target_date = pd.to_datetime(target_date_str)
    df_sorted = df.sort_index()
    try:
        target_loc = df_sorted.index.get_loc(target_date, method='nearest')
        if target_loc < 60: return False, None, None
        curr = df_sorted.iloc[target_loc]
        prev = df_sorted.iloc[target_loc - 1]
        strategy = settings['strategy']
        
        if (curr['Volume'] / 1000) < settings['vol_min']: return False, None, None
        bias = 0
        if strategy != '蜻蜓點水 (縮量回測)':
            if pd.isna(curr['MA200']): return False, None, None
            bias = ((curr['Close'] - curr['MA200']) / curr['MA200']) * 100
            if strategy != '浴火重生 (假跌破)' and abs(bias) > settings['bias_range']: return False, None, None
        
        is_signal = False
        if strategy == '籌碼衝鋒 (集中度高)':
             if (curr['Close'] > curr['MA20']) and (curr['Volume'] > prev['Volume'] * 1.5) and (curr['Close'] > curr['Open']): is_signal = True
        elif strategy == '蜻蜓點水 (縮量回測)':
            if (curr['Close'] > curr['MA200']) and (curr['Low'] <= curr['MA200'] * 1.03) and (curr['Volume'] < curr['Volume_MA5']): is_signal = True
        elif strategy == '浴火重生 (假跌破)':
            if curr['Close'] > curr['MA200']:
                past_7 = df_sorted.iloc[target_loc - 8 : target_loc] 
                is_break = (past_7['Low'] < past_7['MA200']).any()
                if is_break: is_signal = True
        if is_signal: return True, round(bias, 2), target_loc
        else: return False, None, None
    except: return False, None, None

def plot_candlestick(df, signal_date_str, ticker):
    signal_date = pd.to_datetime(signal_date_str)
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05, row_heights=[0.7, 0.3])
    fig.add_trace(go.Candlestick(
        x=df.index, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'],
        name='K線', increasing_line_color='#ef5350', decreasing_line_color='#26a69a'
    ), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['MA20'], line=dict(color='orange', width=1), name='MA20'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['MA60'], line=dict(color='green', width=1), name='MA60'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['MA200'], line=dict(color='blue', width=1.5), name='MA200'), row=1, col=1)
    colors = ['#ef5350' if c >= o else '#26a69a' for c, o in zip(df['Close'], df['Open'])]
    fig.add_trace(go.Bar(x=df.index, y=df['Volume'], name='成交量', marker_color=colors), row=2, col=1)
    try:
        if signal_date in df.index:
            signal_price = df.loc[signal_date, 'High'] * 1.02
            fig.add_trace(go.Scatter(
                x=[signal_date], y=[signal_price],
                mode='markers+text', marker=dict(size=14, color='purple', symbol='triangle-down'),
                text=["黑武士!"], textposition="top center", name=f'訊號日'
            ), row=1, col=1)
    except: pass
    fig.update_layout(title=f"<b>{ticker}</b> 黑武士戰情圖", height=700, xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)

# ==========================================
# 6. 主程式
# ==========================================

try:
    st.title("🔥 黑武士・全能戰情室")
    
    m_temp = get_market_temperature()
    if m_temp:
        col_t1, col_t2 = st.columns(2)
        col_t1.metric("📊 加權指數 (TWII)", m_temp['twii'], m_temp['twii_change'])
        col_t2.metric("😰 恐慌指數 (VIX)", m_temp['vix'], m_temp['vix_change'], delta_color="inverse")
    st.markdown("---")

    st.sidebar.header("🔧 系統診斷 / 通知")
    line_token = st.sidebar.text_input("🔔 Line Notify Token (選填)", type="password")

    if st.sidebar.button("🛠️ 測試連線"):
        with st.sidebar.status("測試中..."):
            try:
                test_df = yf.Ticker("2330.TW").history(period="5d")
                if not test_df.empty: st.write("✅ yfinance OK")
                else: st.error("❌ yfinance Error")
                
                rev_map, rev_date = get_revenue_data_snapshot()
                if rev_map: st.write(f"✅ 營收數據 OK ({rev_date})")
                else: st.warning("⚠️ 營收無資料")
                
                chip_map, d = get_chip_data_snapshot()
                if chip_map: st.write(f"✅ 籌碼 OK ({d})")
                else: st.warning("⚠️ 籌碼無資料")
                
                margin_map = get_margin_data_snapshot()
                if margin_map: st.write(f"✅ 融資 OK")
                else: st.warning("⚠️ 融資無資料")
            except Exception as e: st.error(f"Error: {e}")

    st.sidebar.header("⚔️ 招式選擇")
    strategy_mode = st.sidebar.selectbox("選擇策略：", VALID_STRATEGIES, index=0)
    
    note = ""
    if strategy_mode == "籌碼衝鋒 (集中度高)": note = "★攻擊型：法人買超佔今日成交量 > 10%"
    elif strategy_mode == "蜻蜓點水 (縮量回測)": note = "★防守型：量縮不破！回測年線3%內"
    elif strategy_mode == "浴火重生 (假跌破)": note = "★反轉型：跌破年線後，強勢站回"
    st.sidebar.info(f"💡 **邏輯**：{note}")

    st.sidebar.markdown("---")
    st.sidebar.header("🎯 進階濾網")
    check_trend_high = st.sidebar.checkbox("✅ 前波曾創高 (60日高 > 年線5%)", value=False)
    check_rsi_rising = st.sidebar.checkbox("✅ 動能轉強 (RSI > 昨日)", value=False)
    vol_surge_check = st.sidebar.checkbox("✅ 量能增加 (Vol > 昨日)", value=False)
    check_red_candle = st.sidebar.checkbox("✅ 必須收紅/有撐 (十字/下影)", value=False)
    
    st.sidebar.markdown("---")
    st.sidebar.header("🛡️ 避雷針")
    exclude_negative_pe = st.sidebar.checkbox("✅ 剔除虧損股 (EPS<0 或 PE為負)", value=True)
    exclude_margin_surge = st.sidebar.checkbox("✅ 剔除融資暴增 (散戶>500張)", value=False)
    min_revenue_yoy = st.sidebar.number_input("📉 營收年增率 (YoY) > %", value=-100, step=10, help="預設 -100 表示不過濾")
    
    st.sidebar.markdown("---")
    st.sidebar.header("⚙️ 基礎設定")
    min_vol = st.sidebar.number_input("最低成交量 (張)", value=1000, step=100)
    max_bias = st.sidebar.slider("乖離率範圍 (±%)", 0.1, 10.0, 5.0)
    
    chip_threshold = 10.0
    if strategy_mode == "籌碼衝鋒 (集中度高)":
        st.sidebar.markdown("---")
        chip_threshold = st.sidebar.slider("法人佔成交量 (%)", 5.0, 50.0, 10.0, 5.0)

    settings = {
        'strategy': strategy_mode, 'vol_surge': vol_surge_check, 
        'check_rsi_rising': check_rsi_rising, 'check_trend_high': check_trend_high,
        'check_red_candle': check_red_candle, 'chip_threshold': chip_threshold,
        'vol_min': min_vol, 'bias_range': max_bias, 'chip_flow_surge': False 
    }
    
    debug_stock = st.sidebar.text_input("🕵️‍♂️ 診斷特定股票 (例: 2330)", "")

    if st.sidebar.button("🗑️ 清空所有歷史"):
        clear_history()
        st.sidebar.success("已清空")
        st.rerun() 

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs(["🚀 今日掃描", "📜 歷史紀錄", "📊 基本面健診", "⏳ 單股回測", "📰 個股情報", "🚀 潛力雷達", "🌡️ 資金熱力圖", "🧪 策略實驗室"])

    with tab1:
        st.subheader(f"執行招式：{strategy_mode}")
        if st.button("🔥 啟動掃描 (今日)", type="primary"):
            stock_list = get_tw_stock_list()
            results = []
            chip_map = {}
            margin_map = {}
            rev_map = {}
            
            with st.spinner("集氣中 (下載全市場籌碼、融資、營收)..."):
                chip_map, _ = get_chip_data_snapshot()
                rev_map, _ = get_revenue_data_snapshot()
                if exclude_margin_surge: margin_map = get_margin_data_snapshot()

            bar = st.progress(0.0)
            status_text = st.empty() 
            live_result_placeholder = st.empty() # ★ 修正：使用 placeholder 更新，避免重複顯示
            
            scanned_count = 0
            download_ok = 0
            vol_ok = 0
            total_stocks = len(stock_list)
            
            for i, ticker in enumerate(stock_list):
                prog = min(1.0, (i + 1) / total_stocks)
                bar.progress(prog)
                status_text.text(f"🔥 掃描中... {ticker} | 下載OK: {download_ok} | 量能OK: {vol_ok} | 命中: {len(results)}")
                
                df = fetch_raw_data(ticker, period="1y") 
                if df is None: continue
                download_ok += 1
                
                if df['Volume'].iloc[-1] < (min_vol * 1000): continue
                vol_ok += 1

                df = add_technical_indicators(df)
                if df is None: continue

                match_result = check_stock_strategy_web(df, settings, ticker, chip_map)
                
                if debug_stock and debug_stock in ticker:
                     st.write(f"🔍 [診斷] {ticker} 策略檢查結果: {match_result}")

                if match_result:
                    code = ticker.split('.')[0]
                    # 5. 避雷針檢查
                    if exclude_margin_surge:
                        m_change = margin_map.get(code, 0)
                        if m_change > 500:
                             if debug_stock and debug_stock in ticker: st.write(f"❌ 融資爆增 ({m_change}張) -> 剔除")
                             continue
                    
                    # 6. 營收檢查 (預設 -100 不過濾)
                    rev_data = rev_map.get(code, {'yoy': 0, 'mom': 0})
                    if rev_data['yoy'] < min_revenue_yoy:
                        if debug_stock and debug_stock in ticker: st.write(f"❌ 營收成長不足 ({rev_data['yoy']}%) -> 剔除")
                        continue

                    eps, pe, _ = get_stock_fundamentals_safe(ticker)
                    
                    if exclude_negative_pe:
                        if (eps is not None and eps < 0) or (pe is None):
                             if debug_stock and debug_stock in ticker: st.write(f"❌ 虧損股 (EPS {eps}) -> 剔除")
                             continue
                    
                    is_match, chip_msg = match_result
                    curr = df.iloc[-1]
                    if strategy_mode == "蜻蜓點水 (縮量回測)": bias = 0.0
                    elif pd.isna(curr['MA200']): bias = 0.0
                    else: bias = ((curr['Close'] - curr['MA200']) / curr['MA200']) * 100
                    
                    name = get_stock_name(code)
                    sector = get_stock_sector(code)
                    net_buy = int(chip_map.get(code, 0) / 1000) if chip_map else 0
                    
                    results.append({
                        "代號": code, "名稱": name, "產業": sector,
                        "收盤": round(curr['Close'], 2), 
                        "乖離(%)": round(bias, 2), "量(張)": int(curr['Volume']/1000),
                        "RSI": round(curr['RSI'], 2),
                        "法人買超(張)": net_buy,
                        "營收年增(%)": rev_data['yoy'],
                        "營收月增(%)": rev_data['mom'],
                        "EPS": eps if eps else "N/A",
                        "本益比": pe if pe else "N/A",
                        "資料日期": df.index[-1].strftime('%Y-%m-%d'), "策略": strategy_mode, "籌碼狀態": chip_msg
                    })
                    
                    live_df = pd.DataFrame(results).sort_values(by="RSI", ascending=False)
                    # ★ 修正：使用 placeholder 更新
                    live_result_placeholder.dataframe(
                        live_df,
                        column_config={
                            "RSI": st.column_config.ProgressColumn("RSI", format="%d", min_value=0, max_value=100),
                            "營收年增(%)": st.column_config.NumberColumn("營收年增", format="%.1f%%"),
                        },
                        hide_index=True
                    )
            
            bar.progress(1.0)
            status_text.text("✅ 掃描完成")
            if results:
                st.success(f"掃描完成！發現 {len(results)} 個目標！")
                save_to_history(results)
                
                if line_token:
                    msg = f"\n🔥 黑武士戰報 ({get_taiwan_time().strftime('%m/%d')})\n"
                    msg += f"策略：{strategy_mode}\n發現：{len(results)} 檔\n"
                    for r in results[:3]:
                        msg += f"• {r['名稱']}({r['代號']}): {r['收盤']}元 / YoY {r['營收年增(%)']}%\n"
                    send_line_notify(line_token, msg)
                    st.toast("Line 通知已發送")
            else: 
                st.warning("今日無目標。建議使用側邊欄【診斷工具】檢查連線。")

    with tab2:
        st.header("📜 歷史紀錄 (策略分類版)")
        df_hist = load_history()
        
        if df_hist is not None and not df_hist.empty:
            unique_dates = sorted(df_hist['篩選日期'].unique(), reverse=True)
            for i, date_str in enumerate(unique_dates):
                is_expanded = (i == 0)
                with st.expander(f"📅 {date_str} 掃描紀錄", expanded=is_expanded):
                    df_day = df_hist[df_hist['篩選日期'] == date_str].copy()
                    df_grouped = df_day.groupby(['代號', '名稱']).agg({
                        '策略': lambda x: list(x),
                        '收盤': 'last', 'RSI': 'last', '產業': 'last', 
                        '法人買超(張)': 'last', '營收年增(%)': 'last'
                    }).reset_index()
                    df_grouped['策略數'] = df_grouped['策略'].apply(len)
                    
                    multi_hits = df_grouped[df_grouped['策略數'] > 1].copy()
                    if not multi_hits.empty:
                        multi_hits['符合策略'] = multi_hits['策略'].apply(lambda x: ", ".join(x))
                        st.markdown("#### 🔥 多重共振 (同時符合2種以上)")
                        st.dataframe(multi_hits.drop(columns=['策略', '策略數']), hide_index=True,
                                     column_config={"RSI": st.column_config.ProgressColumn("RSI", min_value=0, max_value=100, format="%d")})
                    
                    st.markdown("#### ⚔️ 單一策略分類")
                    cols = st.columns(3)
                    for idx, strat in enumerate(VALID_STRATEGIES):
                        with cols[idx]:
                            st.write(f"**{strat}**")
                            df_s = df_day[df_day['策略'] == strat].copy()
                            if not df_s.empty:
                                st.dataframe(
                                    df_s[['代號', '名稱', '產業', '收盤', 'RSI', '營收年增(%)']], 
                                    hide_index=True,
                                    column_config={"RSI": st.column_config.ProgressColumn("RSI", min_value=0, max_value=100, format="%d")}
                                )
                            else: st.caption("無資料")
        else: st.info("尚無紀錄")

    with tab3:
        st.header("📊 個股基本面健診")
        c_fund, _ = st.columns([1,2])
        fund_ticker = c_fund.text_input("輸入代號 (例如 2330)", "")
        if c_fund.button("查詢基本面"):
             if fund_ticker:
                 eps, pe, roe = get_stock_fundamentals_safe(fund_ticker)
                 if eps is not None:
                     col_a, col_b, col_c = st.columns(3)
                     col_a.metric("每股盈餘 (EPS)", f"{eps} 元")
                     col_b.metric("本益比 (PE)", f"{pe} 倍")
                     col_c.metric("股東權益報酬率 (ROE)", f"{round(roe*100, 2)}%" if roe else "N/A")
                     st.success(f"{fund_ticker} 數據獲取成功")
                 else: st.error("查無數據")

    with tab4:
        st.header("⏳ 黑武士 - 時光回溯")
        c1, c2 = st.columns([1, 2])
        target_stock = c1.text_input("輸入代號 (回測用)", "2330")
        if c1.button("開始回測"):
            clean_sid = target_stock.replace(".TW", "").replace(".TWO", "").strip()
            ticker = f"{clean_sid}.TW"
            with st.spinner(f"正在回溯 {ticker} 過去 5 年走勢..."):
                df = fetch_stock_data(ticker, period="5y")
            if df is not None and len(df) > 100:
                signals = []
                results = []
                search_start = max(260, 60) if strategy_mode == "蜻蜓點水 (縮量回測)" else max(260, df.index.get_loc(df['MA200'].first_valid_index()))
                for i in range(search_start, len(df)):
                    d_str = df.index[i].strftime('%Y-%m-%d')
                    is_sig, bias, loc = check_signal_on_date(df, d_str, settings, strict_mode=True)
                    if is_sig:
                        signals.append(d_str)
                        ret = calculate_forward_performance(df, loc)
                        price = df.iloc[loc]['Close']
                        results.append({
                            "訊號日期": d_str, "進場價": round(price, 2),
                            "波段最高漲幅": f"{ret['波段最高漲幅(%)']}%",
                            "最高價日期": ret['最高價日期'], "持有天數": ret['持有天數']
                        })
                if results:
                    st.success(f"回測完成！共出現 {len(results)} 次買點。")
                    res_df = pd.DataFrame(results)
                    res_df['漲幅數值'] = res_df['波段最高漲幅'].str.replace('%','').astype(float)
                    res_df = res_df.sort_values('漲幅數值', ascending=False).drop(columns=['漲幅數值'])
                    st.dataframe(res_df)
                    selected_date = st.selectbox("選擇日期查看當時 K 線", signals)
                    plot_candlestick(df, selected_date, clean_sid)
                else: st.warning("無符合訊號。")
            else: st.error("資料不足或無法下載。")

    with tab5:
        st.header("📰 個股與產業情報")
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🔥 個股/產業動態")
            if st.button("更新情報"):
                news_list, keywords = get_all_market_news()
                if news_list:
                    if keywords:
                        kw_count = Counter(keywords).most_common(5)
                        kw_text = " ".join([f"#{k[0]}" for k in kw_count])
                        st.info(f"熱點: {kw_text}")
                    for n in news_list:
                        st.markdown(f"""
                        <div style="padding: 10px; border-bottom: 1px solid #ddd;">
                            <span style="color:gray; font-size:12px;">[{n['來源']}]</span><br>
                            <a href="{n['連結']}" target="_blank" style="font-size: 16px; font-weight:bold;">{n['標題']}</a>
                        </div>
                        """, unsafe_allow_html=True)
                else: st.warning("暫無相關新聞")
        with col2:
            st.subheader("💰 資金流向 (動態變化)")
            if st.button("更新資金流向"):
                main_s, flow_in, flow_out, d_date = get_twse_sector_flow_dynamic()
                if main_s is not None:
                    st.success(f"資料日期: {d_date} (比較昨日變化)")
                    st.write("📈 **資金湧入 (變動率 +%)**")
                    st.dataframe(flow_in, hide_index=True)
                    st.write("📉 **資金撤退 (變動率 -%)**")
                    st.dataframe(flow_out, hide_index=True)
                    st.write("📊 **主流板塊 (成交金額最大)**")
                    st.dataframe(main_s, hide_index=True)
                else: st.error(f"無法取得資料: {d_date}")
        st.markdown("---")
        st.subheader("🏆 法人掃貨榜 (智慧標籤)")
        if st.button("查看法人買超"):
            rank_df, date_str = get_institutional_ranking_smart()
            if rank_df is not None:
                st.success(f"資料日期: {date_str}")
                st.dataframe(rank_df, hide_index=True)
            else: st.error(f"無法取得資料: {date_str}")

    with tab6:
        st.header("🚀 潛力飆股雷達")
        if st.button("啟動雷達偵測"):
            with st.spinner("交叉比對中..."):
                _, flow_in, _, _ = get_twse_sector_flow_dynamic()
                rank_df, _ = get_institutional_ranking_smart()
                news_list, _ = get_all_market_news()
                if flow_in is not None and rank_df is not None:
                    st.success("✅ 分析完成")
                    hot_sectors = flow_in['分類指數名稱'].tolist()
                    st.write(f"🔥 強勢板塊：{', '.join(hot_sectors)}")
                    matches = []
                    for index, row in rank_df.iterrows():
                        stock_name = row['名稱']
                        stock_status = row['狀態']
                        related_news = []
                        for n in news_list:
                            if stock_name in n['標題']: related_news.append(n['標題'])
                        if "爆買" in stock_status or ("連買" in stock_status and len(related_news) > 0):
                            matches.append({
                                "代號": row['代號'], "名稱": stock_name,
                                "狀態": stock_status,
                                "新聞佐證": related_news[0] if related_news else "無",
                                "強度": "⭐⭐⭐" if "爆買" in stock_status else "⭐⭐"
                            })
                    if matches: st.dataframe(pd.DataFrame(matches))
                    else: st.warning("無明顯共振訊號")
                else: st.error("數據不足")

    with tab7:
        st.header("🌡️ 全台股市資金熱力圖 (Max版)")
        st.info("方塊大小 = 成交金額 | 顏色 = 漲跌幅 | 高度已調整為 1200px")
        if st.button("生成熱力圖"):
            with st.spinner("正在抓取全市場數據..."):
                df_heat, date_str = get_tw_market_heatmap_data()
            if df_heat is not None:
                st.success(f"資料日期: {date_str} (前400大成交股)")
                fig = px.treemap(
                    df_heat,
                    path=['產業', '標籤'],
                    values='成交金額',
                    color='漲跌幅%',
                    color_continuous_scale=['#00da3c', '#ffffff', '#ff0000'],
                    color_continuous_midpoint=0,
                    range_color=[-10, 10],
                    title=f"台股資金熱力圖 (細分族群版) - {date_str}"
                )
                fig.update_layout(width=1200, height=900, margin=dict(t=50, l=10, r=10, b=10))
                fig.update_traces(textinfo="label+value", textfont_size=20)
                st.plotly_chart(fig, use_container_width=True)
            else: st.error("無法取得熱力圖數據")

    with tab8:
        st.header("🧪 策略實驗室 (模擬持有至今)")
        st.info("系統將讀取歷史紀錄，模擬「若當初買進持有到今天」的績效。")
        
        if st.button("開始模擬演練"):
            df_hist = load_history()
            
            if df_hist is None or df_hist.empty:
                st.warning("⚠️ 無歷史紀錄，請先掃描。")
            else:
                results = []
                all_data = df_hist.drop_duplicates(subset=['代號', '篩選日期', '策略'])
                total_len = len(all_data)
                bar = st.progress(0.0)
                
                for i, row in all_data.iterrows():
                    bar.progress(min(1.0, (i + 1) / total_len))
                    ticker = f"{row['代號']}.TW"
                    entry_date = row['篩選日期']
                    entry_price = row.get('進場價', row.get('收盤')) 
                    
                    if pd.isna(entry_price): continue
                    
                    try:
                        df_sim = yf.Ticker(ticker).history(period="1y")
                        if df_sim.empty: continue
                        df_sim.index = df_sim.index.tz_localize(None)
                        entry_dt = pd.to_datetime(entry_date)
                        df_hold = df_sim[df_sim.index >= entry_dt]
                        
                        if len(df_hold) < 1: continue 
                        curr_price = df_hold.iloc[-1]['Close']
                        final_pl = ((curr_price - entry_price) / entry_price) * 100
                        
                        results.append({
                            "策略": row['策略'],
                            "代號": row['代號'], "名稱": row['名稱'],
                            "進場日期": entry_date, "進場價": round(entry_price, 2),
                            "現價": round(curr_price, 2), "報酬率(%)": round(final_pl, 2)
                        })
                    except: continue
                
                bar.progress(1.0)
                
                if results:
                    st.success(f"演練完成！共 {len(results)} 筆。")
                    df_res = pd.DataFrame(results)
                    available_strats = df_res['策略'].unique()
                    strat_tabs = st.tabs([f"⚔️ {s}" for s in available_strats])
                    
                    for idx, strat in enumerate(available_strats):
                        with strat_tabs[idx]:
                            df_s = df_res[df_res['策略'] == strat]
                            avg_ret = df_s['報酬率(%)'].mean()
                            win_rate = (df_s['報酬率(%)'] > 0).sum() / len(df_s) * 100
                            max_win = df_s['報酬率(%)'].max()
                            
                            c1, c2, c3, c4 = st.columns(4)
                            c1.metric("交易次數", len(df_s))
                            c2.metric("平均報酬", f"{avg_ret:.2f}%")
                            c3.metric("勝率", f"{win_rate:.1f}%")
                            c4.metric("最高獲利", f"{max_win:.2f}%")
                            st.markdown("---")
                            
                            unique_dates = sorted(df_s['進場日期'].unique(), reverse=True)
                            for d in unique_dates:
                                df_day = df_s[df_s['進場日期'] == d].copy()
                                day_avg = df_day['報酬率(%)'].mean()
                                day_color = "🟢" if day_avg > 0 else "🔴"
                                with st.expander(f"{day_color} {d} (均報酬 {day_avg:.1f}%)"):
                                    st.dataframe(
                                        df_day[['代號', '名稱', '進場價', '現價', '報酬率(%)']],
                                        hide_index=True, use_container_width=True,
                                        column_config={
                                            "報酬率(%)": st.column_config.ProgressColumn(
                                                "損益", format="%.2f%%", min_value=-20, max_value=20
                                            )
                                        }
                                    )
                else: st.warning("無模擬結果")

except Exception as e:
    st.error(f"發生錯誤: {e}")
