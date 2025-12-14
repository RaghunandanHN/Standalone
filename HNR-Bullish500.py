#######################################################################
###################### NIFTY 500 Bullish BOT - Single Pass ######################
#######################################################################

import requests
import json
from bs4 import BeautifulSoup as bs
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import traceback
import re
import pygsheets
import logging
import os

# Setup logging
log_directory = "Logs"
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, datetime.now().strftime('%d-%m-%Y') + " Nifty_500_Bullish.log")
logging.basicConfig(level=logging.INFO, filename=log_file_path, filemode="a", format="%(asctime)s - %(levelname)s - %(message)s")

def log_and_flush(message):
    logging.info(message)
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()

def special_format(n):
    s, *d = str(n).partition(".")
    r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
    return "".join([r] + d)

def run_bullish500_screener():
    """Single pass execution for NIFTY 500 Bullish screener"""
    
    screenType = "Bullish 500"
    screenContents = "NIFTY 500"
    alertCounter = 1
    listi = []
    
    # Authorization
    try:
        gc = pygsheets.authorize(service_account_file="cred.json")
    except Exception as e:
        log_and_flush(f"Failed to authorize pygsheets: {e}")
        return
    
    # Load market cap links
    try:
        dataa = pd.read_csv('mcLinks.csv')
        mydict = dict(zip(dataa['symbol'], dataa['link']))
    except Exception as e:
        log_and_flush(f"Failed to load mcLinks.csv: {e}")
        return
    
    data = {
        'scan_clause': '( {57960} ( latest close > 1 day ago close and 1 day ago close > 2 days ago close and latest macd histogram ( 26 , 12 , 9 ) > 1 day ago macd histogram ( 26 , 12 , 9 ) and [0] 1 hour rsi( 14 ) > [0] 1 hour ema ( [0] 1 hour rsi( 14 ) , 14 ) and [0] 1 hour ema ( [0] 1 hour rsi( 14 ) , 14 ) > [-1] 1 hour ema ( [-1] 1 hour rsi( 14 ) , 14 ) and [0] 1 hour ema ( [0] 1 hour rsi( 14 ) , 14 ) > 55 and weekly rsi( 14 ) > 1 week ago rsi( 14 ) and 1 week ago rsi( 14 ) > 2 weeks ago rsi( 14 ) and latest close > 100 and latest volume > 50000 and latest macd histogram ( 26 , 12 , 9 ) > 1 day ago macd histogram ( 26 , 12 , 9 ) and latest rsi( 14 ) > latest ema ( latest rsi( 14 ) , 14 ) and latest ema ( latest rsi( 14 ) , 14 ) > 1 day ago ema ( 1 day ago rsi( 14 ) , 14 ) and latest ema ( latest rsi( 14 ) , 14 ) > 55 ) ) '
    }
    
    msg1, msg2, msg3 = "", "", ""
    url5, url2, url3, url4 = "", "", "", ""
    
    try:
        with requests.Session() as s:
            current_time = datetime.now()
            formatted_time = current_time.strftime('%H:%M')
            formatted_date = current_time.strftime('%d-%m-%Y')
            
            # Fetch data from Chartink
            r = s.get('https://chartink.com/screener/time-pass-48')
            soup = bs(r.content, 'lxml')
            s.headers['X-CSRF-TOKEN'] = soup.select_one('[name=csrf-token]')['content']
            r = s.post('https://chartink.com/screener/process', data=data).json()
            
            message = f"*** {formatted_date} *** {formatted_time} *** {screenType} *** Alert - {alertCounter} *** \n{screenContents}"
            msg1 = message + " 1 of 3 \n"
            msg2 = " 2 of 3 \n"
            msg3 = " 3 of 3 \n"
            log_and_flush("Messages prepared")
            
            df = pd.DataFrame(r['data'])
            log_and_flush(f"Data frame fetched. df length = {len(df)}")
            
            if len(df) > 0:
                df['Type'] = screenType
                df['Date'] = ''
                df['Time'] = ''
                df['RSI'] = ''
                df['52W_High'] = ''
                df['52W_Low'] = ''
                df['52W_High_diff'] = ''
                df['Date-time'] = ''
                df['link1'] = ''
                df['link2'] = ''
                df['link3'] = ''
                df['PCR'] = ''
                df['MCAP'] = ''
                
                df = df.drop(['sr', 'bsecode'], axis=1)
                df = df.sort_values(by='per_chg', ascending=False)
                df = df.replace(r'[^\w\s]|_', '', regex=True)
                
                print(f"{screenType} {datetime.now()} Count -> {len(df)}")
                watchlistText = ""
                message = message + "\n\n"
                print(" ", sep=' ', end=' ', flush=True)
                scriptCounter = 0
                for index, row in df.iterrows():
                    log_and_flush(f"NSE code = {row['nsecode']}")
                    scriptCounter += 1
                    print(scriptCounter, sep=' ', end=' ', flush=True)                    
                    df.loc[index, 'Time'] = formatted_time
                    df.loc[index, 'Date'] = formatted_date
                    count = listi.count(row['nsecode'])
                    ncode = row['nsecode']
                    watchlistText = watchlistText + f", NSE:{row['nsecode']}"
                    nname = '{:15.15}'.format(row['name'])
                    
                    urlTxt = f"https://in.tradingview.com/chart/qQrGXVOL/?symbol=NSE:{row['nsecode']}"
                    urlTxtInQuotes = f'"{urlTxt}"'
                    linkedText = f" {nname} "
                    vlm = special_format(row['volume'])
                    
                    l1 = mydict.get(row['nsecode'])
                    l1 = f'"{str(l1)}"'
                    vlm = f" {str(vlm)} "
                    pClose = special_format(row['close'])
                    
                    urlTrendlyne = f"http://trendlyne.com/equity/{row['nsecode']}/stock-page/"
                    urlTrendlyneInQuote = f'"{urlTrendlyne}"'
                    closedWithTrendlyneURL = f" {str(pClose)} "
                    
                    # Fetch 52W High/Low from CNBC
                    try:
                        url1 = f"https://www.cnbc.com/quotes/{row['nsecode']}-IN"
                        headers = {'User-agent': 'Mozilla/5.0'}
                        res = requests.get(url1, headers=headers)
                        soup1 = BeautifulSoup(res.content, 'lxml')
                        stri = soup1.prettify()
                        indexi = stri.find("Summary-stat Summary-_52WeekLowDate")
                        if indexi != -1:
                            indexii = indexi + 208
                            ix = indexi - 326
                            hd = stri[ix:indexii]
                            hd = hd.replace(' ', '').replace('\t', '').replace('\n', '')
                            fiftyTwoHigh = hd[0:8]
                            fiftyTwoLow = hd[-8:]
                            formattedHigh1 = fiftyTwoHigh[3:5] + "." + fiftyTwoHigh[0:2] + "." + fiftyTwoHigh[6:]
                            formattedHigh = re.sub(r'[^a-zA-Z0-9 \n\.]', '', formattedHigh1)
                            date_object52H = fiftyTwoHigh[3:5] + "/" + fiftyTwoHigh[0:2] + "/" + fiftyTwoHigh[6:]
                            date_object52H = re.sub(r'[^a-zA-Z0-9 \n\/]', '', date_object52H)
                            df.loc[index, '52W_High'] = date_object52H
                            log_and_flush(f"52W high = {date_object52H}")
                    except:
                        pass
                    
                    # Fetch Market Cap
                    formattedDiff = ""
                    mtxtMcap = ""
                    intMcap = 0
                    
                    try:
                        url21 = l1[1:-1]
                        if len(url21) > 4:
                            html_content = requests.get(url21).text
                            soup2 = BeautifulSoup(html_content, 'html.parser')
                            i52H = soup2.find("div", attrs={"class": "FR nseH52", "id": "sp_yearlyhigh"})
                            fLTP = float(pClose.replace(",", ""))
                            if i52H is not None:
                                fi52H = float(i52H.text)
                                diff52H = 100 - ((fLTP / fi52H) * 100)
                                formattedDiff = str("{:.2f}".format(diff52H)) + "%"
                                df.loc[index, '52W_High_diff'] = formattedDiff
                            
                            mktCap = soup2.find("td", attrs={"class": "nsemktcap bsemktcap"})
                            if mktCap is not None:
                                intMcap = (mktCap.text).replace(",", "")
                                mtxtMcap = special_format((mktCap.text).replace(",", ""))
                                log_and_flush(f"MCap not null = {mtxtMcap}")
                    except:
                        pass
                    
                    # Fetch RSI from 5paisa
                    rsistr = ""
                    try:
                        url1 = f"https://www.5paisa.com/stocks/{row['nsecode']}-share-price"
                        headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                        html_content1 = requests.get(url1, headers=headers).text
                        soup1 = BeautifulSoup(html_content1, 'html.parser')
                        stri2 = soup1.prettify()
                        indexi2 = stri2.find("RSI")
                        if indexi2 != -1:
                            rsiraw = stri2[indexi2+64:indexi2+64+5]
                            rsiraw = special_format(rsiraw)
                            rsistr = rsiraw.replace(" ", "").strip()
                            df.loc[index, 'RSI'] = rsistr
                            log_and_flush(f"RSI not null = {rsistr}")
                    except:
                        pass
                    
                    # Fetch PCR
                    pcrraw = "00"
                    try:
                        pcrUrl = f"https://trendlyne.com/futures-options/derivative/31-jul-2025/{row['nsecode']}"
                        headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                        html_content1 = requests.get(pcrUrl, headers=headers).text
                        soup1 = BeautifulSoup(html_content1, 'html.parser')
                        stri2 = soup1.prettify()
                        indexi2 = stri2.find("PCR VOL")
                        if indexi2 != -1:
                            pcrraw = stri2[indexi2-674: indexi2-665]
                            pcrraw = pcrraw.replace(" ", "").strip()
                    except:
                        pass
                    
                    # Build message
                    if count > 0:
                        if index >= 0 and index <= 20:
                            msg1 = msg1 + f"{ncode} [{count}] | {linkedText} | {row['per_chg']} | {closedWithTrendlyneURL} | {vlm} | {formattedHigh} | {mtxtMcap} | {rsistr} | {formattedDiff} | {pcrraw}\n"
                        elif index >= 21 and index <= 40:
                            msg2 = msg2 + f"{ncode} [{count}] | {linkedText} | {row['per_chg']} | {closedWithTrendlyneURL} | {vlm} | {formattedHigh} | {mtxtMcap} | {rsistr} | {formattedDiff} | {pcrraw}\n"
                        elif index >= 41 and index <= 60:
                            msg3 = msg3 + f"{ncode} [{count}] | {linkedText} | {row['per_chg']} | {closedWithTrendlyneURL} | {vlm} | {formattedHigh} | {mtxtMcap} | {rsistr} | {formattedDiff} | {pcrraw}\n"
                    else:
                        if index >= 0 and index <= 20:
                            msg1 = msg1 + f"{ncode} [{count}] | {linkedText} | {row['per_chg']} | {closedWithTrendlyneURL} | {vlm} | {formattedHigh} | {mtxtMcap} | {rsistr} | {formattedDiff} | {pcrraw}\n"
                        elif index >= 21 and index <= 40:
                            msg2 = msg2 + f"{ncode} [{count}] | {linkedText} | {row['per_chg']} | {closedWithTrendlyneURL} | {vlm} | {formattedHigh} | {mtxtMcap} | {rsistr} | {formattedDiff} | {pcrraw}\n"
                        elif index >= 41 and index <= 60:
                            msg3 = msg3 + f"{ncode} [{count}] | {linkedText} | {row['per_chg']} | {closedWithTrendlyneURL} | {vlm} | {formattedHigh} | {mtxtMcap} | {rsistr} | {formattedDiff} | {pcrraw}\n"
                    
                    listi.append(row['nsecode'])
                    df.loc[index, 'Date-time'] = current_time.strftime("%Y-%m-%d %H:%M:%S")
                    df.loc[index, 'link1'] = urlTxtInQuotes[1:-1]
                    df.loc[index, 'link2'] = urlTrendlyneInQuote[1:-1]
                    df.loc[index, 'link3'] = l1[1:-1]
                    df.loc[index, 'PCR'] = pcrraw
                    df.loc[index, 'MCAP'] = intMcap
                
                # Telegram setup
                TOKEN = '7436753626:AAHyZ5E3BhVCqtK0x-Ih53_oTX1hXrIT8NM'
                URL = f"https://api.telegram.org/bot{TOKEN}/"
                chat_id = "-1002165307620"
                
                # Post to Telegram
                try:
                    if len(msg1) > 0:
                        url5 = URL + f"sendMessage?text={msg1}&chat_id={chat_id}&parse_mode=HTML&disable_web_page_preview=True"
                        requests.get(url5)
                        print(f"\n.....Posted 1 of 3 msg, count = {alertCounter} Time = {datetime.now()}")
                        log_and_flush("msg1 posted")
                    
                    if len(msg2) > 9:
                        url2 = URL + f"sendMessage?text={msg2}&chat_id={chat_id}&parse_mode=HTML&disable_web_page_preview=True"
                        requests.get(url2)
                        print(f"........Posted 2 of 3 msg, count = {alertCounter} Time = {datetime.now()}")
                        log_and_flush("msg2 posted")
                    
                    if len(msg3) > 9:
                        url3 = URL + f"sendMessage?text={msg3}&chat_id={chat_id}&parse_mode=HTML&disable_web_page_preview=True"
                        requests.get(url3)
                        print(f"..........Posted 3 of 3 msg, count = {alertCounter} Time = {datetime.now()}")
                        log_and_flush("msg3 posted")
                except Exception as e:
                    log_and_flush(f"Failed to post to Telegram: {e}")
                
                # Send to Google Sheet
                try:
                    if len(df) > 0:
                        sh = gc.open("hnrtestdb")
                        worksheet = sh[0]
                        cells = worksheet.get_all_values(include_tailing_empty_rows=False, include_tailing_empty=False, returnas='matrix')
                        last_row = len(cells)
                        worksheet.insert_rows(last_row, number=1, values=df.values.tolist())
                        log_and_flush("Data sent to Google Sheet")
                except Exception as e:
                    log_and_flush(f"Failed to send data to sheet: {e}")
            
            else:
                message = message + "\n\n ......NIL"
                log_and_flush("No data found")
                try:
                    url4 = URL + f"sendMessage?text={message}&chat_id={chat_id}&parse_mode=HTML&disable_web_page_preview=True"
                    requests.get(url4)
                except:
                    pass
    
    except Exception as e:
        log_and_flush(f"Exception in main loop: {e}")
        traceback.print_exc()

# Run the screener
if __name__ == "__main__":
    run_bullish500_screener()
