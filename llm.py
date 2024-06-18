# https://llama.meta.com/docs/llama-everywhere/running-meta-llama-on-mac/

import requests
import sqlite3

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

def attempt_llama3():
    num1 = int(llama3("Pick a number from 1 to 10. Say only the number and nothing else."))
    print(num1)
    num2 = int(llama3("Pick a number from 1 to 10, besides 7. Say only the number and nothing else."))
    print(num2)
    print(num1 * num2)

# https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python
def csv_to_sql_hosp_admissions():
    con = sqlite3.connect("admissions.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS admissions;")
    cur.execute("CREATE TABLE admissions (subject_id, hadm_id, admittime, dischtime, deathtime, admission_type, admit_provider_id, admission_location, discharge_location, insurance, language, marital_status, race, edregtime, edouttime, hospital_expire_flag);")
    with open('mimic-iv-2.2/hosp/admissions.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iv-2.2/hosp/admissions.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                print(current_line_num)
                subject_id, hadm_id, admittime, dischtime, deathtime, admission_type, admit_provider_id, admission_location, discharge_location, insurance, language, marital_status, race, edregtime, edouttime, hospital_expire_flag = current_line
                hospital_expire_flag = hospital_expire_flag.split('\n')[0]
                cur.execute("INSERT INTO admissions (subject_id, hadm_id, admittime, dischtime, deathtime, admission_type, admit_provider_id, admission_location, discharge_location, insurance, language, marital_status, race, edregtime, edouttime, hospital_expire_flag) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                            (subject_id, hadm_id, admittime, dischtime, deathtime, admission_type, admit_provider_id, admission_location, discharge_location, insurance, language, marital_status, race, edregtime, edouttime, hospital_expire_flag))
                con.commit()
            current_line_num += 1
    cur = con.cursor()
    cur.execute("SELECT * FROM admissions;")
    for row in cur.fetchall():
        print(row)
    con.close()

def csv_to_sql_hosp_omr():
    con = sqlite3.connect("omr.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS omr;")
    cur.execute("CREATE TABLE omr (subject_id, chartdate, seq_num, result_name, result_value);")
    with open('mimic-iv-2.2/hosp/omr.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iv-2.2/hosp/omr.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                print(current_line_num)
                subject_id, chartdate, seq_num, result_name, result_value = current_line
                result_value = result_value.split('\n')[0]
                cur.execute("INSERT INTO omr (subject_id, chartdate, seq_num, result_name, result_value) VALUES (?, ?, ?, ?, ?);",
                            (subject_id, chartdate, seq_num, result_name, result_value))
                con.commit()
            current_line_num += 1
    cur = con.cursor()
    cur.execute("SELECT * FROM omr;")
    for row in cur.fetchall():
        print(row)
    con.close()

<<<<<<< HEAD
# csv_to_sql_hosp_admissions()
# csv_to_sql_hosp_omr()
=======
def sql_hosp_admissions_to_predict_dischtime():
    con = sqlite3.connect("admissions.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM admissions;")
    for row in cur.fetchall():
        admittime = row[2]
        admission_type = row[5]
        admission_location = row[7]
        insurance = row[9]
        language = row[10]
        marital_status = row[11]
        race = row[12]
        edregtime = row[13]
        predicted_dischtime = llama3(
            f'''Consider a patient whose admission to the hospital is at {admittime}, 
            admission type is {admission_type}, location of admission is 
            {admission_location}, insurance status is {insurance}, 
            language is {language}, marital status is {marital_status}, 
            race is {race}, and emergency room admission is at {edregtime}. 
            What is the patient's discharge time from the hospital? The format of your answer 
            should be in YYYY-MM-DD HH:MM:SS. Do not provide any information except for that 
            answer.'''
        )
        dischtime = row[3]
        print(f"Predicted dischtime: {predicted_dischtime}")
        print(f"Dischtime: {dischtime}")
    con.close()

# csv_to_sql_hosp_admissions()
# csv_to_sql_hosp_omr()
sql_hosp_admissions_to_predict_dischtime()
>>>>>>> 0db5da2 (Added sql_hosp_admissions_to_predict_dischtime() function)
