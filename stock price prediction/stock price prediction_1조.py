import pymysql
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import json, calendar
from threading import Timer

class DBupdate:
    def __init__(self):
        #MySQL 연결
        self.conn = pymysql.connect(host='localhost', user='root', password='1111', db='Investar', charset='utf8')

        #company_info 테이블 생성 (회사명, 종목코드, 최근 업데이트 날짜)
        with self.conn.cursor() as cur:
            sql = """
            CREATE TABLE IF NOT EXISTS company_info (
            company CHAR(40), 
            last_update DATE,
            code CHAR(20),
            PRIMARY KEY (code)) 
            """
            cur.execute(sql)

        #daily_price 테이블 생성 (종목코드, 날짜, 시가, 종가, 전일가, 고가, 저가, 거래량)
        with self.conn.cursor() as cur:    
            sql = """
            CREATE TABLE IF NOT EXISTS daily_price (
            code CHAR(20),
            date DATE,
            open INT,
            close INT,
            diff INT,
            high INT,
            low INT,
            volume INT,
            PRIMARY KEY (code, date))
            """
            cur.execute(sql)

        #변경 내용 저장
        self.conn.commit()

        #update_company_info 함수 호출
        self.update_company_info()
    """생성자"""
    def __del__(self):
        #MySQL 연결 종료
        self.conn.close()
    """소멸자"""
    def read_krx_code(self): 
        #krx에서 상장법인목록 가져오기
        url = "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
        res = requests.get(url)
        krx = pd.read_html(res.text)[0]

        #회사명과 종목코드에 대한 데이터만 추출, 칼럼 이름 변경
        krx = krx[['회사명', '종목코드']] # 데이터프레임 인덱싱 사용하여 회사명, 종목코드 columns만 krx에 저장
        krx = krx.rename(columns={'회사명':'company', '종목코드':'code'} ) # 회사명, 종목코드 column명 rename
        # krx df에서 정수형 6자리로 값을 포맷팅.
        krx.code = krx.code.map('{:06d}'.format)
        # krx(type:DataFrame)
        return krx
        
    def update_company_info(self): 
        # company_info 정보 최신화 
        # [ __init__ 함수에서 마지막에 호출되는 함수]
        with self.conn.cursor() as cur:
            #company_info 테이블에서 최근 업데이트 날짜 검색
            sql = "SELECT MAX(last_update) FROM company_info"
            cur.execute(sql)
            #최근 업데이트 날짜를 sql_date에 저장
            sql_date = cur.fetchone()
            #오늘 날짜를 datetime모듈 사용해서 날짜를  YYYY-mm-dd 형태로 today에 저장
            today = datetime.today().strftime('%Y-%m-%d')
            #만약 업데이트 기록이 없거나 최근 업데이트 날짜가 오늘이 아닐 경우
            if sql_date[0] == None or sql_date[0].strftime('%Y-%m-%d') < today:
                #read_krx_code 함수 호출 >> 최신 상장법인목록 조회 후 company_info 업데이트
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx] #code에 종목코드 저장
                    company = krx.company.values[idx] #company에 회사명 저장
                    #회사명, 종목코드 REPLACE
                    sql = f"REPLACE INTO company_info VALUES ('{company}', '{code}', '{today}')"
                    cur.execute(sql)
                    self.conn.commit()  
    
    def read_price(self, code, pages_to_fetch): 
        # 네이버 금융에서 해당 기업 주식 정보 가져오기
        url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
        headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36'}
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'lxml')
        #페이지 정보 저장
        try:
            # 첫 pgRR태그를 가지는 값에서 '=' split 후 가장 마지막 column값을 호출
            last_page = int(soup.select_one('td.pgRR').a['href'].split('=')[-1])
        except:
            # 총 페이지가 1인 경우 [pgRR] 부재 >> 오류 발생
            last_page = 1
        
        #빈 데이터프레임 생성
        total = pd.DataFrame()
        
        last_page = min(last_page, pages_to_fetch)
        #페이지 별 주식 정보를 total에 병합
        for page in range(1, last_page + 1):
            res = requests.get(f"{url}&page={page}", headers=headers)
            soup = BeautifulSoup(res.text, 'lxml')
            soup_table = soup.select('table')
            # table에 'soup_table' DataFrame 저장.NaN값은 제거
            table = pd.read_html(str(soup_table))[0].dropna()
            # pd.concat()함수를 사용하여 total,table DataFrame 합쳐서 total에 저장
            total = pd.concat([total, table]) 
            
        
        # total DateFrame -1씩 슬라이싱해서 가장마지막에 추가된 값부터 출력
        total = total[::-1]
        # 칼럼 이름 변경
        total = total.rename(columns={'날짜':'Date', '시가':'Open', '종가':'Close', '전일비':'Diff', '고가':'High', '저가':'Low', '거래량':'Volume'})
        # 날짜로 변환
        total.Date = pd.to_datetime(total.Date)
        # 결측값(NaN) 제거
        total = total.dropna()
        # 정수 타입으로 변환, DataFrame.astype(int)
        total[['Close', 'Diff', 'Open', 'High', 'Low', 'Volume']] = total[['Close', 'Diff', 'Open', 'High', 'Low', 'Volume']].astype(int)
        # total DataFrame 기존 인덱스를 drop하고 새 인덱스 재배열
        total = total.reset_index(drop=True)
        total = total[['Date', 'Close', 'Diff', 'Open', 'High', 'Low', 'Volume']]
        total['code'] = code # code 컬럼 추가

        return total

    def update_daily_price(self, pages_to_fetch):
        # company_info 테이블에서 회사명과 종목코드 검색
        cur = self.conn.cursor()
        cur.execute("SELECT company, code FROM company_info")
        # codes에 모두 저장
        codes = cur.fetchall()

        # 네이버 금융에서 해당기업 주식 정보 조회 후 replace_daily_price 함수 호출
        for idx, code in enumerate(codes):
            df = self.read_price(code[1], pages_to_fetch )
            if df is None:
                continue
            self.replace_daily_price(df)
            if idx == 50: # 인덱스 값을 50으로 임의 설정하여 50번째까지만 실행
                break
   
   
    def replace_daily_price(self, df):
        """ 종목코드와 주식 정보를 REPLACE"""
        # code에 column명 code의 값을 DataFrame형태로 선언
        code = df['code'][0]
        with self.conn.cursor() as cur:
            # 튜플에 대한 순환반복 index = {code}, columns = date,close,open,diff,high,low,vol
            for row in df.itertuples():
                sql = f"REPLACE INTO daily_price VALUES ('{code}', \
                    '{row.Date}', {row.Close}, {row.Open}, {row.Diff}, {row.High}, {row.Low}, {row.Volume})"
                cur.execute(sql)
        self.conn.commit()
        
        idx=6
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] #{idx+1} ({code}) : {len(df)} row >>> REPLACE [COMPLETED]")

    def daily_update(self): #매일 오후 17시에 daily_prcie 테이블 업데이트
        #update_company_info 함수 호출 
        self.update_company_info()
        
        #pages_to_fetch(페이지 수) 설정
        try:
            # DB 업데이트 후 생성되는 config.json을 읽어오기
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError: #json 파일이 없는 경우 생성
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 10 
                config = {'pages_to_fetch': 1}
                json.dump(config, out_file)
        #update_daily_price 함수 호출
        self.update_daily_price(pages_to_fetch)
        
        #현재 날짜와 시간을 tmnow에 저장
        tmnow = datetime.now() 
        #calendar.monthrange() : 특정 연도와 특정 월의 마지막 요일과 마지막 일자를 튜플 형태로 반환
        #이번 달 마지막 일자를 lastday에 저장
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        
        #tmnext 설정(월,일값에 따른 파라미터 값 변경 매일 17시 설정)
        if tmnow.month == 12 and tmnow.day == lastday: 
            tmnext = tmnow.replace(year=tmnow.year+1, month=1, day=1, hour=17, minute=0, second=0)
        elif tmnow.day == lastday: 
            tmnext = tmnow.replace(montho=tmnow.month+1, day=1, hour=17, minute=0, second=0)
        else:
            tmnext = tmnow.replace(month=tmnow.day+1, hour=17, minute=0, second=0)
        
        #tmnext와 tmnow의 시간차를 tmdiff에 저장
        tmdiff = tmnext - tmnow
        #tmdiff를 초단위로 변환
        secs = tmdiff.seconds
        #secs만큼 타이머 설정 >> 매일 17시마다 daily_update 함수 실행
        t = Timer(secs, self.daily_update)
        #다음 업데이트 시간 공지
        print(f"Waiting for next update ({tmnext.strftime('%Y-%m-%d %H:%M')}")
        #타이머 시작
        t.start()

    def read_Company_stock_Info(self,company_code, start_d, end_d) : 

        conn = pymysql.connect(host='localhost',user='root',password='1111', db='INVESTAR',charset='utf8')
        cur = conn.cursor()

        sql="show tables" # company_infotbl
        print(sql)
        cur.execute(sql)
        rows = cur.fetchall()
        print(rows)
        

        sql=f"select * from daily_price where (DATE(date) BETWEEN '{start_d}' AND '{end_d}') ;"
        # WHERE DATE(post_date) BETWEEN '2012-01-22' AND '2012-01-23'
        print(sql)
        cur.execute(sql)    
        rows = cur.fetchall()
        #### tuple tupe 에서 Dataframe으로 변환
        
        rows = list(rows)
        res=pd.DataFrame(rows)
        res=res[res[0] == company_code]
        print(res.head)

        # conn.close()

        return res

    def startTimer(self):
        # 10초 주기 타이머 작동
        print("Timer")
        self.update_daily_price(1)
        # 파라미터(sec, self.startTimer)사용으로 지속호출
        timer = Timer(10, self.startTimer)
        print("Waiting for next run" ) 
        timer.start()
        

if __name__ == '__main__':
    dbu = DBupdate()
    
    
    dbu.daily_update()

    res = dbu.read_price('005930', 3) # 작동
    print(type(res))
    print(res)
 
    df =res
    # idx=10
    # code=
    dbu.replace_daily_price(df) # 작동
    '''
    # dbu.update_daily_price(3)
    
    # 종목조회 
    # company_code ='000860'
    # dbu.read_Company_stock_Info(company_code,'2021-12-24', '2022-01-14') 
    
    # dbu.startTimer()
    # dbu.daily_routine()
    '''