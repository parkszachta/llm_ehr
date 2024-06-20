# https://llama.meta.com/docs/llama-everywhere/running-meta-llama-on-mac/

import requests
import sqlite3
from datetime import datetime

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
    con.commit()
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
    con.commit()
    con.close()

def csv_to_sql_hosp_drgcodes():
    con = sqlite3.connect("drgcodes.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS drgcodes;")
    cur.execute("CREATE TABLE drgcodes (subject_id, hadm_id, drg_type, drg_code, description, drg_severity, drg_mortality);")
    with open('mimic-iv-2.2/hosp/drgcodes.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iv-2.2/hosp/drgcodes.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                print(current_line_num)
                print(current_line)
                subject_id = current_line[0]
                hadm_id = current_line[1]
                drg_type = current_line[2]
                drg_code = current_line[3]
                description = ""
                i = 4
                while i < len(current_line) - 3:
                    description += current_line[i]
                    description += ","
                    i += 1
                description += current_line[len(current_line) - 3]
                drg_severity = current_line[len(current_line) - 2]
                drg_mortality = current_line[len(current_line) - 1]
                drg_mortality = drg_mortality.split('\n')[0]
                cur.execute("INSERT INTO drgcodes (subject_id, hadm_id, drg_type, drg_code, description, drg_severity, drg_mortality) VALUES (?, ?, ?, ?, ?, ?, ?);",
                            (subject_id, hadm_id, drg_type, drg_code, description, drg_severity, drg_mortality))
                con.commit()
            current_line_num += 1
    cur = con.cursor()
    cur.execute("SELECT * FROM drgcodes;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def sql_hosp_admissions_to_predict_dischtime():
    con = sqlite3.connect("admissions.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM admissions;")
    for row in cur.fetchall():
        subject_id = row[0]
        admittime = row[2]
        admission_type = row[5]
        admission_location = row[7]
        insurance = row[9]
        language = row[10]
        marital_status = row[11]
        race = row[12]
        edregtime = row[13]
        con2 = sqlite3.connect("drgcodes.db")
        cur2 = con2.cursor()
        cur2.execute("SELECT description FROM drgcodes WHERE subject_id = ?;", (subject_id,))
        description = ""
        for row2 in cur2.fetchall():
            description += row2[0]
        # These all resulted in very incorrect answers
        # Zero-shot prompting
        predicted_dischtime = llama3(
            f'''Consider a patient whose admission to the hospital in YYYY-MM-DD HH:MM:SS format 
            is at {admittime}, admission type is {admission_type}, location of admission is 
            {admission_location}, insurance status is {insurance}, language is {language}, 
            marital status is {marital_status}, race is {race}, emergency room admission is at 
            {edregtime}, and description is {description}. Predict the patient's discharge time 
            from the hospital. The format of your answer should be in YYYY-MM-DD HH:MM:SS. 
            Do not provide any information except for that answer.'''
        )
        # Chain-of-thought prompting
        '''
        predicted_dischtime = llama3(
            f"Consider a patient whose admission to the hospital in YYYY-MM-DD HH:MM:SS format 
            is at {admittime}, admission type is {admission_type}, location of admission is 
            {admission_location}, insurance status is {insurance}, language is {language}, 
            marital status is {marital_status}, race is {race}, emergency room admission is at 
            {edregtime}, and description is {description}. Predict the patient's discharge time 
            from the hospital. The format of your answer should be in YYYY-MM-DD HH:MM:SS. 
            Do not provide any information except for that answer. Make sure to think step-by-step
            in detail!"
        )
        '''
        admittime = datetime.strptime(admittime, '%Y-%m-%d %H:%M:%S')
        dischtime = datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')
        predicted_dischtime = datetime.strptime(predicted_dischtime, '%Y-%m-%d %H:%M:%S')
        length_of_stay = dischtime - admittime
        predicted_length_of_stay = predicted_dischtime - admittime
        print(f"------")
        print(f"Admittime: {admittime}")
        print(f"Dischtime: {dischtime}")
        print(f"Predicted dischtime: {predicted_dischtime}")
        print(f"Length of stay: {length_of_stay}")
        print(f"Predicted length of stay: {predicted_length_of_stay}")
        print(f"Ratio length of stay: {predicted_length_of_stay / length_of_stay}")
    con.commit()
    con.close()

def csv_to_sql_hosp_omr_summary():
    con = sqlite3.connect("omr_summary.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS omr_summary;")
    cur.execute("CREATE TABLE omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height);")
    with open('mimic-iv-2.2/hosp/omr.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iv-2.2/hosp/omr.csv', 'r') as file:
        current_line_num = 1
        subject_id = ""
        total_systolic = 0
        total_diastolic = 0
        amt_blood_pressure = 0
        total_weight = 0
        amt_weight = 0
        total_bmi = 0
        amt_bmi = 0
        total_height = 0
        amt_height = 0
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                print(current_line_num)
                new_subject_id, _, _, result_name, result_value = current_line
                result_value = result_value.split('\n')[0]
                if new_subject_id != subject_id:
                    # If we have moved onto a new subject, put the averages from the old subject into omr_summary
                    if subject_id != "":
                        avg_systolic = "N/A" if amt_blood_pressure == 0 else round(total_systolic / amt_blood_pressure)
                        avg_diastolic = "N/A" if amt_blood_pressure == 0 else round(total_diastolic / amt_blood_pressure)
                        avg_weight = "N/A" if amt_weight == 0 else round(total_weight / amt_weight, 2)
                        avg_bmi = "N/A" if amt_bmi == 0 else round(total_bmi / amt_bmi, 1)
                        avg_height = "N/A" if amt_height == 0 else round(total_height / amt_height, 2)
                        cur.execute("INSERT INTO omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES (?, ?, ?, ?, ?, ?);", 
                                    (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height))
                    # Reset the tracking variables when we are onto a subject we have not yet tracked
                    subject_id = new_subject_id
                    total_systolic = 0
                    total_diastolic = 0
                    amt_blood_pressure = 0
                    total_weight = 0
                    amt_weight = 0
                    total_bmi = 0
                    amt_bmi = 0
                    total_height = 0
                    amt_height = 0
                # Update tracking variables
                if result_name == "Blood Pressure":
                    blood_pressure = result_value.split('/')
                    total_systolic += float(blood_pressure[0])
                    total_diastolic += float(blood_pressure[1])
                    amt_blood_pressure += 1
                elif result_name == "Weight (Lbs)":
                    total_weight += float(result_value)
                    amt_weight += 1
                elif result_name == "BMI (kg/m2)":
                    total_bmi += float(result_value)
                    amt_bmi += 1
                elif result_name == "Height (Inches)":
                    total_height += float(result_value)
                    amt_height += 1
                con.commit()
            current_line_num += 1
    # Put the averages from the final patient into omr_summary
    avg_systolic = "N/A" if amt_blood_pressure == 0 else round(total_systolic / amt_blood_pressure)
    avg_diastolic = "N/A" if amt_blood_pressure == 0 else round(total_diastolic / amt_blood_pressure)
    avg_weight = "N/A" if amt_weight == 0 else round(total_weight / amt_weight, 2)
    avg_bmi = "N/A" if amt_bmi == 0 else round(total_bmi / amt_bmi, 1)
    avg_height = "N/A" if amt_height == 0 else round(total_height / amt_height, 2)
    cur.execute("INSERT INTO omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES (?, ?, ?, ?, ?, ?);", 
                (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height))
    cur = con.cursor()
    cur.execute("SELECT * FROM omr_summary;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def csv_to_sql_hosp_d_icd_diagnoses():
    con = sqlite3.connect("d_icd_diagnoses.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS d_icd_diagnoses;")
    cur.execute("CREATE TABLE d_icd_diagnoses (icd_code, icd_version, long_title);")
    with open('mimic-iv-2.2/hosp/d_icd_diagnoses.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iv-2.2/hosp/d_icd_diagnoses.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            print(current_line)
            if current_line_num > 1:
                icd_code = current_line[0]
                icd_version = current_line[1]
                long_title = current_line[2]
                i = 3
                while i < len(current_line):
                    long_title += ","
                    long_title += current_line[i]
                    i += 1
                long_title = long_title.split('\n')[0]
                cur.execute("INSERT INTO d_icd_diagnoses (icd_code, icd_version, long_title) VALUES (?, ?, ?);", 
                    (icd_code, icd_version, long_title))
            current_line_num += 1
    cur = con.cursor()
    cur.execute("SELECT * FROM d_icd_diagnoses;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def csv_to_sql_hosp_diagnoses_icd_diabetes():
    con = sqlite3.connect("d_icd_diagnoses.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM d_icd_diagnoses WHERE (long_title LIKE '%diabetes%' OR long_title LIKE '%Diabetes%')")
    diabetes_icd_codes_and_icd_versions = []
    for row in cur.fetchall():
        icd_code = row[0]
        icd_version = row[1]
        diabetes_icd_codes_and_icd_versions.append([icd_code, icd_version])
    con.close()
    con = sqlite3.connect("diagnoses_icd_diabetes.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS diagnoses_icd_diabetes;")
    cur.execute("CREATE TABLE diagnoses_icd_diabetes (icd_code, icd_version);")
    for icd_code, icd_version in diabetes_icd_codes_and_icd_versions:
        cur.execute("INSERT INTO diagnoses_icd_diabetes (icd_code, icd_version) VALUES (?, ?);", 
                    (icd_code, icd_version))
    cur = con.cursor()
    cur.execute("SELECT * FROM diagnoses_icd_diabetes;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def csv_to_sql_hosp_diabetes():
    con = sqlite3.connect("diabetes.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS diabetes;")
    cur.execute("CREATE TABLE diabetes (subject_id, diagnosed_with_diabetes);")
    with open('mimic-iv-2.2/hosp/diagnoses_icd.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iv-2.2/hosp/diagnoses_icd.csv', 'r') as file:
        subject_id = ""
        diagnosed_with_diabetes = 0
        current_line_num = 1
        while current_line_num <= total_lines:
            print(current_line_num)
            current_line = file.readline().split(',')
            if current_line_num > 1:
                new_subject_id, _, _, icd_code, icd_version = current_line
                icd_version = icd_version.split('\n')[0]
                if new_subject_id != subject_id:
                    if subject_id != "":
                        cur.execute("INSERT INTO diabetes (subject_id, diagnosed_with_diabetes) VALUES (?, ?);",
                                    (subject_id, diagnosed_with_diabetes))
                        con.commit()
                        print(f"Subject_id: {subject_id}, Diagnosed_with_diabetes: {diagnosed_with_diabetes}")
                    subject_id = new_subject_id
                    diagnosed_with_diabetes = 0
                con2 = sqlite3.connect("diagnoses_icd_diabetes.db")
                cur2 = con2.cursor()
                cur2.execute("SELECT * FROM diagnoses_icd_diabetes WHERE (icd_code = ? AND icd_version = ?);",
                            (icd_code, icd_version))
                if cur2.fetchone() is not None:
                    diagnosed_with_diabetes = 1
                con2.commit()
            current_line_num += 1
        cur.execute("INSERT INTO diabetes (subject_id, diagnosed_with_diabetes) VALUES (?, ?);",
                                (subject_id, diagnosed_with_diabetes))
        con.commit()
        print(f"Subject_id: {subject_id}, Diagnosed_with_diabetes: {diagnosed_with_diabetes}")
    cur = con.cursor()
    cur.execute("SELECT * FROM diabetes;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con2.commit()
    con.close()
    con2.close()

# csv_to_sql_hosp_admissions()
# csv_to_sql_hosp_omr()
# csv_to_sql_hosp_drgcodes()
# sql_hosp_admissions_to_predict_dischtime()
# csv_to_sql_hosp_omr_summary()
# csv_to_sql_hosp_d_icd_diagnoses()
# csv_to_sql_hosp_diagnoses_icd_diabetes()
csv_to_sql_hosp_diabetes()