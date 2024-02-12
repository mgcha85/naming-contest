import requests
from bs4 import BeautifulSoup
import pandas as pd
from database import *
from time import sleep
import re
import subprocess


def get_contest_details(url):
    # Send a GET request to the URL
    response = requests.get(url)
    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    # Extract the detailed information
    detail_info = soup.find('div', class_='your-detail-class').text  # Update the class as per actual HTML structure
    return detail_info


def remove_emoji(text):
    emoji_pattern = re.compile("["
        "\U0001F000-\U0001F6FF"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)


def find_bracket_contents(s):
# '['로 시작하는 첫 번째 인덱스 찾기
    start_index = s.find('[')
    # ']'로 끝나는 첫 번째 인덱스 찾기
    end_index = s.rfind(']')
    
    # 둘 다 존재하는 경우에만 결과 반환
    if start_index != -1 and end_index != -1 and end_index > start_index:
        return s[start_index:end_index+1]
    else:
        return ''
    
# Docker 컨테이너를 실행하고 출력을 받는 함수
def run_docker_and_capture_output(url):
    cmd = [
        "docker", "run", "-i", "f574d8a6c9ba", "python", "/app/ocr.py",
        "--url", f"{url}"
    ]
    
    # subprocess를 사용하여 Docker 커맨드 실행
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # 표준 출력에서 결과를 받음
    output = result.stdout
    
    return output

def fetch_contest_data(url, offset, limit=39):
    # URL from which the JSON data will be fetched
    url = "https://api.stunning.kr/api/v1/dantats/contest"
    # Payload as query parameters
    payload = {
        "categoryKey": "contest.common.johnnygrimes",
        "state": "End",
        "orderBy": "winnerdate",
        "offset": offset,
        "limit": limit,
        "secret": "N"
    }
    
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6,zh;q=0.5",
        "Origin": "https://www.loud.kr",
        "Pragma": "no-cache",
        "Referer": "https://www.loud.kr/",
        "Sec-Ch-Ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Linux"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "Uid": "",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "X-Csrf-Token": "6e5a6238314e466c75666b315a4d57544d41626679504f43766f794e7139534a42576d49596c4f564954554f312f336d4a316c6f547a6d2b50692f3563446b716a4954697431716471527242683172534a76366874493145363030725352624a4575426f5543714873666e78686356712f4946544d774d37316561777a5673345465346d48567046566634672f6755504a6655396f55726d55784a69307a486d33714b74427179526c3243706443594a413269334b5674777254344650345732596e307262616f4a6f6e5239484c6d686a6d3137752b764b4e3433696f7833326d574a6a61504a3778616b3d",
        "X-Seo-Key": "/contest/list/idea/end",
        "X-Service-Clause": "loud"
    }
    # Making a GET request to the URL
    response = requests.get(url, headers=headers, params=payload)
    
    # Checking if the request was successful
    if response.status_code == 200:
        # Parsing the JSON response
        data = response.json()
        return data
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None


def get_detail(data):
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6,zh;q=0.5",
        "Origin": "https://www.loud.kr",
        "Pragma": "no-cache",
        "Referer": "https://www.loud.kr/",
        "Sec-Ch-Ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "Uid": "7770045b-9fca-4fec-8a92-147cb6d79f09",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "X-Csrf-Token": "Your_X-Csrf-Token_Value_Here",
        "X-Seo-Key": "/contest/view/122833/brief",
        "X-Service-Clause": "loud"
    }

    url_ = 'https://api.stunning.kr/api/v1/dantats/contest/{cid}/portfolio/{pid}'
    cid = data.name
    
    cols = [x for x in data.index if 'portfolioID' in x]
    for each in data[cols].dropna():
        sleep(0.25)
        url = url_.format(cid=cid, pid=each)
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                # Parse the JSON response
                data = response.json()
                
                description = []
                for x in data['resultData']['portfolio']['contents']:
                    if 'content' in x:
                        description.append(remove_emoji(x['content']))
                    elif x['type'] == 'Image' and 'files' in x:
                        for y in x['files']:
                            output = run_docker_and_capture_output(y['url'])
                            output = find_bracket_contents(output)
                            description.append(remove_emoji("\n".join(eval(output))))
                        
                return '\n\n'.join(description)
            else:
                print(f"Failed to fetch data. Status code: {response.status_code}")
                return None
        except Exception as e:
            return


def get_contest_meta(url):
    # Send a GET request to the URL
    response = requests.get(url)
    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    # Find all <li> elements containing contests
    contests = soup.find_all('li')
    meta_info_list = []

    for contest in contests:
        # Extracting the detail URL
        detail_url_suffix = contest.find('a')['href']
        detail_url = f"https://www.loud.kr{detail_url_suffix}"
        
        # Extracting the image URL
        image_url = contest.find('img')['src']
        
        # Extracting the title
        title_tag = contest.find('p', class_='sc-ZyCDH dFGcIH')
        title = title_tag['title'] if title_tag and title_tag.has_attr('title') else title_tag.text
        
        # Extracting the description
        description = contest.find('div', class_='desc').text.strip()
        # Extracting the total prize and number of entries
        total_prize = contest.find('div', class_='total-prize').find('span', class_='value').text.strip()
        join_count = contest.find('div', class_='join-count').find('span', class_='value').text.strip()
        
        # Extracting the label if present
        label = contest.find('div', class_='sc-jZiqTT lHHry')
        label_text = label.text.strip() if label else None
        
        # Compiling the contest meta information
        meta_info = {
            'detail_url': detail_url,
            'image_url': image_url,
            'title': title,
            'description': description,
            'total_prize': total_prize,
            'join_count': join_count,
            'label': label_text
        }
        
        meta_info_list.append(meta_info)

    return meta_info_list


def processText(html_content):
    soup = BeautifulSoup(html_content, 'lxml')
    # Extract text
    return remove_emoji(soup.get_text(separator=' ', strip=True))


if __name__ == '__main__':   
    # Replace 'your-url' with the actual URL    
    n_items = 2500
    offset = 0
    limit = 39
    db = Database()
    out_columns = ['startDate', 'recruitEndDate',  'endDate', 'title', 'totalPrize', 'nick', 'company', 'companyDescription', 'content', 'ans_title0', 'ansDescription']

    # dfList = []
    while offset < n_items:
        print("offset: ", offset)
        meta_info_list = fetch_contest_data('https://www.loud.kr/contest/list/idea/end', offset, limit)
        df = pd.DataFrame(meta_info_list['resultData'])
        if 'id' not in df:
            offset += limit
            continue

        dfs = [df[['id', 'startDate', 'recruitEndDate',  'endDate', 'title', 'totalPrize']].reset_index(drop=True)]
        for col in ['client', 'clientInfo', 'contestInfo', 'joinCondition', 'briefing', 'prizes']:
            data = pd.DataFrame(df[col].tolist())
            dfs.append(data)

        df_merge = pd.concat(dfs, axis=1)
        dfs = [df_merge.set_index('id')]
        dfs.append(pd.DataFrame([x[0] for x in df_merge['contents'].dropna().tolist()]).set_index('contestID'))
        
        for col in data.columns:
            each = df_merge[col].dropna().tolist()
            df = pd.DataFrame(each)
            portfolio_df = pd.DataFrame(df['portfolio'].dropna().tolist())
            df = df.drop('portfolio', axis=1).merge(portfolio_df, left_on='portfolioID', right_on='id').set_index('contestID')
            dfs.append(df[['title', 'rank', 'winAt', 'portfolioID']].rename(columns={'title': f'ans_title{col}', 'rank': f'rank{col}', 'winAt': f'winAt{col}', 'portfolioID': f'portfolioID{col}'})) 

        df_merge = pd.concat(dfs, axis=1).drop(data.columns, axis=1).fillna('')
        df_merge['ansDescription'] = df_merge.apply(get_detail, axis=1)

        for col in ['content', 'ansDescription']:
            df_merge[col] = df_merge[col].apply(processText)

        df_merge = df_merge.reset_index().rename(columns={'index': 'id'})
        columns = [x for x in out_columns if x in df_merge.columns]
        df_merge = df_merge[columns].fillna('')
        # df_merge.to_excel(f"data/training_data_{offset}.xlsx", index=False)

        # df_merge.to_sql("training_data", db.engine, index=False, if_exists='append', schema='loudsourcing')
        for idx, row in df_merge.iterrows():
            try:
                db.insert_by_series("loudsourcing.training_data", row)
            except:
                pass

        offset += limit
        sleep(0.25)

        # dfList.append(df_merge)

    # df_merge = pd.concat(dfs)
    # df_merge.to_sql("loudsourcing.training_data", db.engine, if_exists='append', index=False)

    # for meta in meta_info_list:
    #     print(f"Title: {meta['title']}, Detail URL: {meta['detail_url']}")
    #     # Fetch and print detail information for each contest
    #     detail_info = get_contest_details(meta['detail_url'])
    #     print(f"Details: {detail_info}\n")
