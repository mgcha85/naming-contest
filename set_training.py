from database import Database
import pandas as pd
import json

db = Database()
df = pd.read_sql("select * from loudsourcing.training_data", db.engine)

output_format = """
name: {}
description: {}
"""

input_format = """
company: {}
company description: {}
task description: {}
"""

contents = []
for idx, row in df.iterrows():
    company = row['company'].strip()
    companydescription = row['companydescription'].strip()
    content = row['content'].strip()

    input = input_format.format(company, companydescription, content)

    ans_title = row['ans_title0'].strip()
    ansdescription = row['ansdescription'].strip()
    
    output = output_format.format(ans_title, ansdescription)

    instruction = "Create awesome name and description for my product or company"
    contents.append({"input": input, "output": output, "instruction": instruction})

with open("training.json", 'w', encoding='utf8') as f:
    json.dump(contents, f, ensure_ascii=False)
