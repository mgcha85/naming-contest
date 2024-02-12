import pandas as pd
from glob import glob
from database import Database
from bs4 import BeautifulSoup
from time import sleep
import re


# get all files in path
flist = glob('data/*.xlsx')
df = pd.concat([pd.read_excel(fpath) for fpath in flist])
df = df.dropna(subset=['ansDescription'])


def remove_emoji(text):
    emoji_pattern = re.compile("["
        "\U0001F000-\U0001F6FF"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)


def processText(html_content):
    soup = BeautifulSoup(html_content, 'lxml')
    # Extract text
    return remove_emoji(soup.get_text(separator=' ', strip=True))


for col in ['content', 'ansDescription']:
    df[col] = df[col].apply(processText)

db = Database()
df = df.fillna('')

# df.to_sql("training_data", db.engine, index=False, if_exists='append', schema='loudsourcing')
for idx, row in df.iterrows():
    sleep(0.1)
    try:
        db.insert_by_series("loudsourcing.training_data", row)
    except Exception as e:
        print(idx)
        pass
