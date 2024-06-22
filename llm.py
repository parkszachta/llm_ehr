# https://llama.meta.com/docs/llama-everywhere/running-meta-llama-on-mac/

import requests
import sqlite3
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn import svm
from sklearn import metrics

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

# omr_summary_filtered has rows of (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height)
# diabetes_filtered has rows of (subject_id, diagnosed_with_diabetes)
# these do not have any patients with at least one 'N/A'
def sql_to_sql_hosp_diabetes_filtered():
    con = sqlite3.connect("omr_summary.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM omr_summary WHERE (avg_systolic != 'N/A' AND avg_diastolic != 'N/A' AND avg_weight != 'N/A' AND avg_bmi != 'N/A' AND avg_height != 'N/A');")
    con2 = sqlite3.connect("diabetes.db")
    cur2 = con2.cursor()
    con3 = sqlite3.connect("omr_summary_filtered.db")
    cur3 = con3.cursor()
    cur3.execute("DROP TABLE IF EXISTS omr_summary_filtered;")
    cur3.execute("CREATE TABLE omr_summary_filtered (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height);")
    con4 = sqlite3.connect("diabetes_filtered.db")
    cur4 = con4.cursor()
    cur4.execute("DROP TABLE IF EXISTS diabetes_filtered;")
    cur4.execute("CREATE TABLE diabetes_filtered (subject_id, diagnosed_with_diabetes);")
    current_line_num = 1
    for row in cur.fetchall():
        print(current_line_num)
        current_subject_id = row[0]
        avg_systolic = row[1]
        avg_diastolic = row[2]
        avg_weight = row[3]
        avg_bmi = row[4]
        avg_height = row[5]
        cur2.execute("SELECT * FROM diabetes WHERE subject_id = ?", 
                    (current_subject_id,))
        fetched = cur2.fetchone()
        if fetched is not None:
            diagnosed_with_diabetes = fetched[0]
            cur3.execute("INSERT INTO omr_summary_filtered (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES (?, ?, ?, ?, ?, ?);", 
                        (current_subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height))
            cur4.execute("INSERT INTO diabetes_filtered (subject_id, diagnosed_with_diabetes) VALUES (?, ?);", 
                        (current_subject_id, diagnosed_with_diabetes))
        current_line_num += 1
    cur3.execute("SELECT * FROM omr_summary_filtered;")
    for row in cur3.fetchall():
        print(row)
    cur4.execute("SELECT * FROM diabetes_filtered;")
    for row in cur4.fetchall():
        print(row)
    con.commit()
    con2.commit()
    con3.commit()
    con4.commit()
    con.close()
    con2.close()
    con3.close()
    con4.close()

def sql_hosp_diabetes_filtered_to_svm():
    features = []
    labels = []
    con = sqlite3.connect("omr_summary_filtered.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM omr_summary_filtered;")
    con2 = sqlite3.connect("diabetes_filtered.db")
    cur2 = con2.cursor()
    cur2.execute("SELECT * FROM diabetes_filtered;")
    current_line_num = 1
    for row in cur.fetchall():
        print(current_line_num)
        avg_systolic = row[1]
        avg_diastolic = row[2]
        avg_weight = row[3]
        avg_bmi = row[4]
        avg_height = row[5]
        diagnosed_with_diabetes = cur2.fetchone()[1]
        features.append([avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height])
        labels.append(diagnosed_with_diabetes)
        current_line_num += 1
    con.commit()
    con2.commit()
    con.close()
    con2.close()
    print(f"Features length: {len(features)}")
    print(f"First few features: {features[0:20]}")
    print(f"Labels length: {len(labels)}")
    print(f"First few labels: {labels[0:20]}")
    X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.2)
    clf = svm.SVC(kernel='linear')
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)
    print(f"Predicted labels length: {len(y_pred)}")
    print(f"First few predicted labels: {y_pred[0:20]}")
    print(f"Accuracy: {metrics.accuracy_score(y_test, y_pred)}")
    print(f"Precision: {metrics.precision_score(y_test, y_pred)}")
    print(f"Recall: {metrics.recall_score(y_test, y_pred)}")

def admitted_patients():
    con = sqlite3.connect("admissions.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM admissions;")
    con2 = sqlite3.connect("admitted_patients.db")
    cur2 = con2.cursor()
    cur2.execute("DROP TABLE IF EXISTS admitted_patients;")
    cur2.execute("CREATE TABLE admitted_patients (subject_id);")
    current_subject_id = ""
    for row in cur.fetchall():
        subject_id = row[0]
        if subject_id != current_subject_id:
            if current_subject_id != "":
                cur2.execute("INSERT INTO admitted_patients (subject_id) VALUES (?);",
                            (current_subject_id,))
            current_subject_id = subject_id
    cur2.execute("INSERT INTO admitted_patients (subject_id) VALUES (?);",
                (current_subject_id,))
    con2.commit()
    con.commit()
    cur2.execute("SELECT * FROM admitted_patients;")
    for row in cur2.fetchall():
        print(row)
    con2.commit()
    con.close()
    con2.close()

def patients_icd10_codes():
    con = sqlite3.connect("patients_icd10_codes.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS patients_icd10_codes;")
    cur.execute("CREATE TABLE patients_icd10_codes (subject_id, hadm_id, icd10_code);")
    with open('mimic-iv-2.2/hosp/diagnoses_icd.csv', 'r') as file:
        total_lines = len(file.readlines())
    current_subject_id = ""
    current_icd10_codes = []
    with open('mimic-iv-2.2/hosp/diagnoses_icd.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            print(current_line)
            subject_id = current_line[0]
            current_hadm_id = current_line[1]
            icd_code = current_line[3]
            icd_version = current_line[4].split('\n')[0]
            if subject_id != current_subject_id:
                if current_subject_id != "":
                    for current_icd10_code in current_icd10_codes:
                        cur.execute("INSERT INTO patients_icd10_codes (subject_id, hadm_id, icd10_code) VALUES (?, ?, ?);", 
                                    (current_subject_id, current_hadm_id, current_icd10_code))
                current_subject_id = subject_id
                current_icd10_codes = []
            if icd_version == "10" and icd_code not in current_icd10_codes:
                current_icd10_codes.append(icd_code)
            current_line_num += 1
    for current_icd10_code in current_icd10_codes:
        cur.execute("INSERT INTO patients_icd10_codes (subject_id, hadm_id, icd10_code) VALUES (?, ?);", 
                    (current_subject_id, current_hadm_id, current_icd10_code))
    con.commit()
    cur.execute("SELECT * FROM patients_icd10_codes;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def icd10_to_phecodes():
    con = sqlite3.connect('icd10_to_phecodes.db')
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS icd10_to_phecodes")
    cur.execute(f"CREATE TABLE icd10_to_phecodes (ICD, phecode);")
    with open('ICD-CM to phecode, unrolled.txt') as file:
        amt_of_icd10_codes = len(file.readlines())
    with open('ICD-CM to phecode, unrolled.txt') as file:
        current_line_num = 1
        while current_line_num <= amt_of_icd10_codes:
            current_line = file.readline().split('\t')
            print(current_line)
            if current_line_num > 1:
                icd_code_split = current_line[0].split('.')
                ICD = ""
                for icd_code_piece in icd_code_split:
                    if icd_code_piece != ".":
                        ICD += icd_code_piece
                flag = int(current_line[1])
                phecode = current_line[2].split('\n')[0]
                if flag == 10:
                    cur.execute("INSERT INTO icd10_to_phecodes (ICD, phecode) VALUES (?, ?);", 
                                (ICD, phecode))
            current_line_num += 1
    cur.execute("SELECT * FROM icd10_to_phecodes;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def icd10_to_phecodes_data_structure():
    con = sqlite3.connect('icd10_to_phecodes.db')
    cur = con.cursor()
    cur.execute("SELECT * FROM icd10_to_phecodes;")
    icd10_to_phecodes = {}
    for row in cur.fetchall():
        ICD = row[0]
        phecode = row[1]
        if ICD not in icd10_to_phecodes:
            icd10_to_phecodes[ICD] = set()
        icd10_to_phecodes[ICD].add(phecode)
        print(f"Phecode: {phecode}")
    return icd10_to_phecodes

def hadm_id_to_dischtimes_data_structure():
    con = sqlite3.connect('admissions.db')
    cur = con.cursor()
    cur.execute("SELECT * FROM admissions;")
    hadm_id_to_dischtimes = {}
    for row in cur.fetchall():
        hadm_id = row[1]
        dischtime = row[3]
        hadm_id_to_dischtimes[hadm_id] = dischtime
    return hadm_id_to_dischtimes

def patients_phecodes_dischtimes_sql_hosp():
    # Make a list of the different phecodes
    with open('phecode_definitions1.2.csv', 'r') as file:
        amt_of_phecodes = len(file.readlines())
    phecodes = set()
    with open('phecode_definitions1.2.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= amt_of_phecodes:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                phecode = current_line[0].split('\"')[1]
                phecodes.add(phecode)
                print(phecode)
            current_line_num += 1
    # Make a SQL database, patients_phecodes, such that each phecode is a column
    phecodes_string = ""
    default_string = ""
    for phecode in phecodes:
        phecodes_string += f"`{phecode}`, "
        default_string += "0, "
    phecodes_string = phecodes_string.removesuffix(", ")
    default_string = default_string.removesuffix(", ")
    print(phecodes_string)
    print(default_string)
    con = sqlite3.connect("patients_phecodes_dischtimes.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS patients_phecodes_dischtimes;")
    print(f"CREATE TABLE patients_phecodes_dischtimes (subject_id, {phecodes_string});")
    cur.execute(f"CREATE TABLE patients_phecodes_dischtimes (subject_id, {phecodes_string});")
    # Fill in the rows of patients_phecodes such that each row is a patient
    con2 = sqlite3.connect("admitted_patients.db")
    cur2 = con2.cursor()
    cur2.execute("SELECT * FROM admitted_patients;")
    for row in cur2.fetchall():
        current_subject_id = row[0]
        print(f"Current_subject_id: {current_subject_id}")
        cur.execute(f"INSERT INTO patients_phecodes_dischtimes VALUES (?, {default_string});", 
                    (current_subject_id,))
    # Map each subject_id to their phecodes
    icd10_to_phecodes = icd10_to_phecodes_data_structure()
    hadm_id_to_dischtimes = hadm_id_to_dischtimes_data_structure()
    cur = con.cursor()
    con3 = sqlite3.connect("patients_icd10_codes.db")
    cur3 = con3.cursor()
    cur3.execute("SELECT * FROM patients_icd10_codes;")
    subject_id_to_phecodes_dischtimes = {}
    for row in cur3.fetchall():
        current_subject_id = row[0]
        hadm_id = row[1]
        icd10_code = row[2]
        if icd10_code in icd10_to_phecodes:
            current_phecodes = icd10_to_phecodes[icd10_code]
            for phecode in current_phecodes:
                dischtime = hadm_id_to_dischtimes[hadm_id]
                if phecode in phecodes and (current_subject_id not in subject_id_to_phecodes_dischtimes or tuple([phecode, dischtime]) not in subject_id_to_phecodes_dischtimes[current_subject_id]):
                    if current_subject_id not in subject_id_to_phecodes_dischtimes:
                        subject_id_to_phecodes_dischtimes[current_subject_id] = set()
                    subject_id_to_phecodes_dischtimes[current_subject_id].add(tuple([phecode, dischtime]))
    # Fill in each row with dischtime for each phecode the row's patient has
    for current_subject_id in subject_id_to_phecodes_dischtimes.keys():
        current_phecodes_dischtimes = subject_id_to_phecodes_dischtimes[current_subject_id]
        current_update_phrase = ""
        for current_phecode_dischtime in current_phecodes_dischtimes:
            phecode = current_phecode_dischtime[0]
            dischtime = current_phecode_dischtime[1]
            current_update_phrase += f"`{phecode}` = '{dischtime}', "
        current_update_phrase = current_update_phrase.removesuffix(", ")
        cur.execute(f"UPDATE patients_phecodes_dischtimes SET {current_update_phrase} WHERE subject_id = {current_subject_id};")
        print(f"Current_subject_id: {current_subject_id}")
    con.commit()
    con2.commit()
    con3.commit()
    con2.close()
    con3.close()
    cur.execute("SELECT * FROM patients_phecodes_dischtimes")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

# csv_to_sql_hosp_admissions()
# csv_to_sql_hosp_omr()
# csv_to_sql_hosp_drgcodes()
# sql_hosp_admissions_to_predict_dischtime()
# csv_to_sql_hosp_omr_summary()
# csv_to_sql_hosp_d_icd_diagnoses()
# csv_to_sql_hosp_diagnoses_icd_diabetes()
# csv_to_sql_hosp_diabetes()
# sql_to_sql_hosp_diabetes_filtered()
# sql_hosp_diabetes_filtered_to_svm() This one has a seg fault for some reason
admitted_patients()
patients_icd10_codes()
icd10_to_phecodes()
patients_phecodes_dischtimes_sql_hosp()