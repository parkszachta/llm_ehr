# https://llama.meta.com/docs/llama-everywhere/running-meta-llama-on-mac/

import requests
import csv, sqlite3

url = "http://localhost:11434/api/chat"

def llama3(prompt):
    data = {
        "model": "llama3",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "stream": False
    }
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, json=data)
    return(response.json()['message']['content'])


def try_llama3():
    num1 = int(llama3("Pick a number from 1 to 10. Say only the number and nothing else."))
    print(num1)
    num2 = int(llama3("Pick a number from 1 to 10, besides 7. Say only the number and nothing else."))
    print(num2)
    print(num1 * num2)

# https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python
def csv_to_sql_hosp_admissions():
    con = sqlite3.connect("sqlite:///mimic-iv/hosp/admissions.csv.gz")
    cur = con.cursor()
    cur.execute("CREATE TABLE t (subject_id, hadm_id, admittime, dischtime, deathtime, admission_type, admit_provider_id, admission_location, discharge_location, insurance, language, marital_status, race, edregtime, edouttime, hospital_expire_flag);")
    with open('data.csv','r') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['subject_id'], i['hadm_id'], i['admittime'], i['dischtime'], i['deathtime'], i['admission_type'], i['admit_provider_id'], i['admission_location'], i['discharge_location'], i['insurance'], i['language'], i['marital_status'], i['race'], i['edregtime'], i['edouttime'], i['hospital_expire_flag']) for i in dr]
    cur.executemany("INSERT INTO t (subject_id, hadm_id, admittime, dischtime, deathtime, admission_type, admit_provider_id, admission_location, discharge_location, insurance, language, marital_status, race, edregtime, edouttime, hospital_expire_flag) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", to_db)
    con.commit()
    con.close()