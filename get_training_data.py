import requests
from bs4 import BeautifulSoup
import pandas as pd
from database import *
from time import sleep


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
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJrX3QySG5BcmltR0hGcWNPRTNHWUFURVZxUnQ5ZnpaYU83b0hSaFJrVUpVIn0.eyJleHAiOjE3MDc2MTIxODMsImlhdCI6MTcwNzUyNTc4MywianRpIjoiMjE0MjRlMmUtNDRmNi00MGRiLTg0ZjMtNjIxODQyZDUwMzU3IiwiaXNzIjoiaHR0cHM6Ly9rZXljbG9hay5zdHVubmluZy5rci9hdXRoL3JlYWxtcy9TdHVubmluZyIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI3NzcwMDQ1Yi05ZmNhLTRmZWMtOGE5Mi0xNDdjYjZkNzlmMDkiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJMb3VkIiwic2Vzc2lvbl9zdGF0ZSI6ImRhNjE1ZTA2LWZjNWQtNDdkMi1hMzZhLTAzYjEwNWY0MTczYiIsImFsbG93ZWQtb3JpZ2lucyI6WyIiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImRlZmF1bHQtcm9sZXMtc3R1bm5pbmciLCJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJlbWFpbCBwcm9maWxlIG9mZmxpbmVfYWNjZXNzIExvdWQiLCJzaWQiOiJkYTYxNWUwNi1mYzVkLTQ3ZDItYTM2YS0wM2IxMDVmNDE3M2IiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwicHJlZmVycmVkX3VzZXJuYW1lIjoibWdjaGE4NUBnbWFpbC5jb20iLCJlbWFpbCI6Im1nY2hhODVAZ21haWwuY29tIn0.PzwDl2FzjDa-OrjqjZp8fmUaolZPvsEjR8ggB1Xx7LJC03U0hHdvuGlBOdpsxWRaywattztRstqRFDY5I9w8_oaG3mTQoVeoGzWxmjnRzl-SqRu3agcyqq7Qh54CpuJQ7L5W-b9UFXffXNYMxJOY3-9LXc56b7tx2bRxuIPrpW-buZPpeOHE-Obcl74yKzFxvwmP3bOnAtVsiTLK6mdJrIrkMGv6GxyISkOYeq-n4wQkIZwhZiJiuFPqlCPRePRlPjn_sKVn4PNVWgIMuhjMPwANfxIQOwNnoo_yrY7c45KooAqMIiOlzJ_rMcw7z8o8aaTKP9Zhp2m1aCCHh5SH6g",
        "Origin": "https://www.loud.kr",
        "Pragma": "no-cache",
        "Referer": "https://www.loud.kr/",
        "Sec-Ch-Ua": '"Not A Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "Uid": "7770045b-9fca-4fec-8a92-147cb6d79f09",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "X-Csrf-Token": "7a4f6872434d49443354744e453243593361715a564e71676c422b4d68634f357874426765475931795436476f6837753076334b324749774866306e643554324f58394e314f3772747752694e4a346a634d562b704749315538454d7a507a42697942694f2b452f4f766a514d7334763848464c74746b5a30743954557032336a386862484d7674586f467035486d7858674b7351724e45644d534171364e636949786353376374436a506563766c7768396956326a2f306a2b3771754361642b51746e535a657a43777255344d7a6c5838447a41656c6d74546e385956346e7036306c70372b7a557857304153586f4d68496f674e4b774f5153592f697844",
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
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJrX3QySG5BcmltR0hGcWNPRTNHWUFURVZxUnQ5ZnpaYU83b0hSaFJrVUpVIn0.eyJleHAiOjE3MDc2MTIxODMsImlhdCI6MTcwNzUyNTc4MywianRpIjoiMjE0MjRlMmUtNDRmNi00MGRiLTg0ZjMtNjIxODQyZDUwMzU3IiwiaXNzIjoiaHR0cHM6Ly9rZXljbG9hay5zdHVubmluZy5rci9hdXRoL3JlYWxtcy9TdHVubmluZyIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI3NzcwMDQ1Yi05ZmNhLTRmZWMtOGE5Mi0xNDdjYjZkNzlmMDkiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJMb3VkIiwic2Vzc2lvbl9zdGF0ZSI6ImRhNjE1ZTA2LWZjNWQtNDdkMi1hMzZhLTAzYjEwNWY0MTczYiIsImFsbG93ZWQtb3JpZ2lucyI6WyIiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImRlZmF1bHQtcm9sZXMtc3R1bm5pbmciLCJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJlbWFpbCBwcm9maWxlIG9mZmxpbmVfYWNjZXNzIExvdWQiLCJzaWQiOiJkYTYxNWUwNi1mYzVkLTQ3ZDItYTM2YS0wM2IxMDVmNDE3M2IiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwicHJlZmVycmVkX3VzZXJuYW1lIjoibWdjaGE4NUBnbWFpbC5jb20iLCJlbWFpbCI6Im1nY2hhODVAZ21haWwuY29tIn0.PzwDl2FzjDa-OrjqjZp8fmUaolZPvsEjR8ggB1Xx7LJC03U0hHdvuGlBOdpsxWRaywattztRstqRFDY5I9w8_oaG3mTQoVeoGzWxmjnRzl-SqRu3agcyqq7Qh54CpuJQ7L5W-b9UFXffXNYMxJOY3-9LXc56b7tx2bRxuIPrpW-buZPpeOHE-Obcl74yKzFxvwmP3bOnAtVsiTLK6mdJrIrkMGv6GxyISkOYeq-n4wQkIZwhZiJiuFPqlCPRePRlPjn_sKVn4PNVWgIMuhjMPwANfxIQOwNnoo_yrY7c45KooAqMIiOlzJ_rMcw7z8o8aaTKP9Zhp2m1aCCHh5SH6g",
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
        sleep(0.5)
        url = url_.format(cid=cid, pid=each)
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                # Parse the JSON response
                data = response.json()
                try:
                    data = '\n\n'.join([x['content'] for x in data['resultData']['portfolio']['contents'] if 'content' in x])
                except:
                    return ''
                return data
            else:
                print(f"Failed to fetch data. Status code: {response.status_code}")
                return None
        except:
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


def get_contest_details(url):
    # Send a GET request to the URL
    response = requests.get(url)
    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    # Extract the detailed information
    detail_info = soup.find('div', class_='your-detail-class').text  # Update the class as per actual HTML structure
    return detail_info


if __name__ == '__main__':   
    # Replace 'your-url' with the actual URL    
    n_items = 20000
    offset = 2379
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
        
        df_merge = df_merge.reset_index().rename(columns={'index': 'id'})
        columns = [x for x in out_columns if x in df_merge.columns]
        df_merge = df_merge[columns].fillna('')
        df_merge.to_excel(f"data/training_data_{offset}.xlsx", index=False)

        offset += limit
        sleep(0.5)

        # df_merge.to_sql("training_data", db.engine, index=False, if_exists='append', schema='loudsourcing')
        # for idx, row in df_merge.iterrows():
        #     sleep(0.1)
        #     try:
        #         db.insert_by_series("loudsourcing.training_data", row)
        #     except:
        #         pass

        # dfList.append(df_merge)

    # df_merge = pd.concat(dfs)
    # df_merge.to_sql("loudsourcing.training_data", db.engine, if_exists='append', index=False)

    # for meta in meta_info_list:
    #     print(f"Title: {meta['title']}, Detail URL: {meta['detail_url']}")
    #     # Fetch and print detail information for each contest
    #     detail_info = get_contest_details(meta['detail_url'])
    #     print(f"Details: {detail_info}\n")
