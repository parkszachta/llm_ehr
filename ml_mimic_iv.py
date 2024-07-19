import numpy as np
import sqlite3
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn import svm as svm_callee
from sklearn import metrics
from sklearn import linear_model
import time

def calculate_demographics_helper(subject_ids_total, subject_ids_excluded):
    gender_list = []
    anchor_age_list = []
    with open('mimic-iv-2.2/hosp/patients.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iv-2.2/hosp/patients.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            subject_id = current_line[0]
            if current_line_num > 1 and subject_id in subject_ids_total and subject_id not in subject_ids_excluded:
                gender_list.append(current_line[1])
                anchor_age_list.append(int(current_line[2]))
            current_line_num += 1
    gender_counts = np.unique(gender_list, return_counts=True)
    num_F = gender_counts[1][0]
    num_M = gender_counts[1][1]
    num_people_gender = len(gender_list)
    print(f"Number of people for gender: {num_people_gender}")
    print(f"Number of 'F': {num_F}")
    print(f"Proportion of 'F': {num_F / num_people_gender}")
    print(f"Number of 'M': {num_M}")
    print(f"Proportion of 'M': {num_M / num_people_gender}")
    num_people_anchor_age = len(anchor_age_list)
    print(f"Number of people for anchor age: {num_people_anchor_age}")
    print(f"Mean anchor age: {np.mean(anchor_age_list)}")
    print(f"Standard deviation anchor age: {np.std(anchor_age_list)}")
    con = sqlite3.connect("admitted_patients.db")
    cur = con.cursor()
    subject_ids_excluded = repr(subject_ids_excluded)
    subject_ids_excluded = "()" if subject_ids_excluded == "set()" else subject_ids_excluded.replace('{','(').replace('}',')')
    cur.execute(f"SELECT race FROM admitted_patients WHERE subject_id NOT IN {subject_ids_excluded};")
    race_list = []
    for row in cur.fetchall():
        race_list.append(row[0])
    race_counts = np.unique(race_list, return_counts=True)
    print(race_counts)
    num_people_race = np.sum(race_counts[1])
    names_of_race_categories = race_counts[0]
    counts_of_race_categories = race_counts[1]
    black_categories = ["BLACK/AFRICAN", "BLACK/AFRICAN AMERICAN", "BLACK/CAPE VERDEAN", "BLACK/CARIBBEAN ISLAND"]
    black_count = 0
    white_categories = ["WHITE", "WHITE - BRAZILIAN", "WHITE - EASTERN EUROPEAN", "WHITE - OTHER EUROPEAN", "WHITE - RUSSIAN"]
    white_count = 0
    other_count = 0
    for i in range(len(names_of_race_categories)):
        name_of_race_category = names_of_race_categories[i]
        count_of_race_category = counts_of_race_categories[i]
        if name_of_race_category in black_categories:
            black_count += count_of_race_category
        elif name_of_race_category in white_categories:
            white_count += count_of_race_category
        else:
            other_count += count_of_race_category
    print(f"Number of people for race: {num_people_race}")
    print(f"Number of BLACK: {black_count}")
    print(f"Proportion of BLACK: {black_count / num_people_race}")
    print(f"Number of WHITE: {white_count}")
    print(f"Proportion of WHITE: {white_count / num_people_race}")
    print(f"Number of Other: {other_count}")
    print(f"Proportion of Other: {other_count / num_people_race}")

def calculate_demographics():
    con = sqlite3.connect("admitted_patients.db")
    cur = con.cursor()
    cur.execute("SELECT subject_id FROM admitted_patients;")
    admitted_patients = cur.fetchall()
    admitted_patients_temp = set()
    for item in admitted_patients:
        admitted_patients_temp.add(item[0])
    admitted_patients = admitted_patients_temp
    con2 = sqlite3.connect("patients_phecodes_dischtimes.db")
    cur2 = con2.cursor()
    cur2.execute("SELECT subject_id FROM patients_phecodes_dischtimes;")
    subject_ids = cur2.fetchall()
    print(subject_ids)
    print("subject_ids before ^^^")
    subject_ids_temp = set()
    for item in subject_ids:
        print(f"item: {item}")
        print(f"item[0]: {item[0]}")
        subject_ids_temp.add(item[0])
    subject_ids = subject_ids_temp.intersection(admitted_patients)
    cur2.execute("SELECT subject_id FROM patients_phecodes_dischtimes WHERE `157` = 0;")
    subject_ids_not_diagnosed_with_157 = cur2.fetchall()
    subject_ids_not_diagnosed_with_157_temp = set()
    for item in subject_ids_not_diagnosed_with_157:
        print(f"item: {item}")
        print(f"item[0]: {item[0]}")
        subject_ids_not_diagnosed_with_157_temp.add(item[0])
    subject_ids_not_diagnosed_with_157 = subject_ids_not_diagnosed_with_157_temp.intersection(admitted_patients)
    subject_ids_diagnosed_with_157 = subject_ids - subject_ids_not_diagnosed_with_157
    print(subject_ids)
    print("subject_ids ^^^")
    print(subject_ids_not_diagnosed_with_157)
    print("subject_ids_not_diagnosed_with_157 ^^^")
    print(subject_ids_diagnosed_with_157)
    print("subject_ids_diagnosed_with_157 ^^^")
    print("OVERALL DATA:")
    calculate_demographics_helper(subject_ids, set())
    print("NOT DIAGNOSED WITH `157`:")
    calculate_demographics_helper(subject_ids, subject_ids_diagnosed_with_157)
    print("DIAGNOSED WITH `157`:")
    calculate_demographics_helper(subject_ids, subject_ids_not_diagnosed_with_157)

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

def patients_gender_and_anchor_age():
    con = sqlite3.connect("patients_gender_and_anchor_age.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS patients_gender_and_anchor_age;")
    cur.execute("CREATE TABLE patients_gender_and_anchor_age (subject_id, gender, anchor_age);")
    batch_data = []
    with open('mimic-iv-2.2/hosp/patients.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iv-2.2/hosp/patients.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            print(current_line_num)
            current_line = file.readline().split(',')
            if current_line_num > 1:
                subject_id, gender, anchor_age, _, _, _ = current_line
                anchor_age = int(anchor_age)
                batch_data.append([subject_id, gender, anchor_age])
                print(f"NOT YET: INSERT INTO patients_gender_and_anchor_age (subject_id, gender, anchor_age) VALUES ({subject_id}, {gender}, {anchor_age})")
            current_line_num += 1
    cur.executemany("INSERT INTO patients_gender_and_anchor_age (subject_id, gender, anchor_age) VALUES (?, ?, ?);",
                    batch_data)
    cur = con.cursor()
    cur.execute("SELECT * FROM patients_gender_and_anchor_age;")
    print(cur.fetchall())
    con.commit()
    con.close()

def admitted_patients():
    con = sqlite3.connect("admissions.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM admissions;")
    con2 = sqlite3.connect("admitted_patients.db")
    cur2 = con2.cursor()
    cur2.execute("DROP TABLE IF EXISTS admitted_patients;")
    cur2.execute("CREATE TABLE admitted_patients (subject_id, marital_status, race);")
    current_subject_id = ""
    for row in cur.fetchall():
        subject_id = row[0]
        if subject_id != current_subject_id:
            marital_status = row[11]
            race = row[12]
            cur2.execute("INSERT INTO admitted_patients (subject_id, marital_status, race) VALUES (?, ?, ?);",
                (subject_id, marital_status, race))
            current_subject_id = subject_id
    con2.commit()
    con.commit()
    cur2.execute("SELECT * FROM admitted_patients;")
    for row in cur2.fetchall():
        print(row)
    con2.commit()
    con.close()
    con2.close()

def csv_to_sql_hosp_omr_summary(phecode=-1):
    batch_data = []
    con = sqlite3.connect("omr_summary.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS omr_summary;")
    cur.execute("CREATE TABLE omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height);")
    con2 = sqlite3.connect("patients_phecodes_dischtimes.db")
    cur2 = con2.cursor()
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
        dischtime = 0
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                print(current_line_num)
                new_subject_id, chartdate, _, result_name, result_value = current_line
                chartdate = datetime.strptime(chartdate, '%Y-%m-%d')
                result_value = result_value.split('\n')[0]
                dischtime = 0
                if phecode != -1:
                    print(new_subject_id)
                    print(f"'{new_subject_id}'")
                if new_subject_id != subject_id:
                    # If we have moved onto a new subject, put the averages from the old subject into omr_summary
                    if subject_id != "":
                        avg_systolic = "N/A" if amt_blood_pressure == 0 else round(total_systolic / amt_blood_pressure)
                        avg_diastolic = "N/A" if amt_blood_pressure == 0 else round(total_diastolic / amt_blood_pressure)
                        avg_weight = "N/A" if amt_weight == 0 else round(total_weight / amt_weight, 2)
                        avg_bmi = "N/A" if amt_bmi == 0 else round(total_bmi / amt_bmi, 1)
                        avg_height = "N/A" if amt_height == 0 else round(total_height / amt_height, 2)
                        batch_data.append([subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height])
                        print(f"NOT YET: INSERT INTO omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES ({subject_id}, {avg_systolic}, {avg_diastolic}, {avg_weight}, {avg_bmi}, {avg_height});")
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
                    cur2.execute(f"SELECT `{phecode}` FROM patients_phecodes_dischtimes WHERE subject_id = '{subject_id}';")
                    read_line = cur2.fetchone()
                    if read_line is None:
                        current_line_num += 1
                        continue
                    dischtime = read_line[0]
                    if dischtime != 0:
                        dischtime = datetime.strptime(dischtime, '%Y-%m-%d %H:%M:%S')
                # Update tracking variables
                if dischtime == 0 or chartdate < dischtime:
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
            current_line_num += 1
    # Put the averages from the final patient into omr_summary
    avg_systolic = "N/A" if amt_blood_pressure == 0 else round(total_systolic / amt_blood_pressure)
    avg_diastolic = "N/A" if amt_blood_pressure == 0 else round(total_diastolic / amt_blood_pressure)
    avg_weight = "N/A" if amt_weight == 0 else round(total_weight / amt_weight, 2)
    avg_bmi = "N/A" if amt_bmi == 0 else round(total_bmi / amt_bmi, 1)
    avg_height = "N/A" if amt_height == 0 else round(total_height / amt_height, 2)
    batch_data.append([subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height])
    print(f"NOT YET: INSERT INTO omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES ({subject_id}, {avg_systolic}, {avg_diastolic}, {avg_weight}, {avg_bmi}, {avg_height});")
    cur = con.cursor()
    cur.execute("SELECT * FROM omr_summary;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    cur = con.cursor()
    con3 = sqlite3.connect("admitted_patients.db")
    cur3 = con3.cursor()
    cur3.execute("SELECT subject_id FROM admitted_patients;")
    not_in_omr_summary = []
    for row in cur2.fetchall():
        subject_id = row[0]
        print(subject_id)
        cur.execute("SELECT * FROM omr_summary WHERE subject_id = ?;", (subject_id,))
        if len(cur.fetchall()) == 0:
            not_in_omr_summary.append(subject_id)
    for subject_id in not_in_omr_summary:
        batch_data.append([subject_id, "N/A", "N/A", "N/A", "N/A", "N/A"])
        print(f"NOT YET: INSERT INTO omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES ({subject_id}, {avg_systolic}, {avg_diastolic}, {avg_weight}, {avg_bmi}, {avg_height});")
    con.commit()
    cur.executemany("INSERT INTO omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES (?, ?, ?, ?, ?, ?);",
                    batch_data)
    print("Just inserted for real")
    con.commit()
    con.close()
    con2.commit()
    con2.close()

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

def patients_icd_codes():
    con = sqlite3.connect(f"patients_icd9_codes.db")
    cur = con.cursor()
    cur.execute(f"DROP TABLE IF EXISTS patients_icd9_codes;")
    cur.execute(f"CREATE TABLE patients_icd9_codes (subject_id, hadm_id, icd9_code);")
    con2 = sqlite3.connect(f"patients_icd10_codes.db")
    cur2 = con2.cursor()
    cur2.execute(f"DROP TABLE IF EXISTS patients_icd10_codes;")
    cur2.execute(f"CREATE TABLE patients_icd10_codes (subject_id, hadm_id, icd10_code);")
    with open('mimic-iv-2.2/hosp/diagnoses_icd.csv', 'r') as file:
        total_lines = len(file.readlines())
    current_subject_id = ""
    current_hadm_id = ""
    current_icd_codes_and_flags = []
    with open('mimic-iv-2.2/hosp/diagnoses_icd.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                print(current_line)
                subject_id = current_line[0]
                hadm_id = current_line[1]
                icd_code = current_line[3]
                flag = int(current_line[4].split('\n')[0])
                if hadm_id != current_hadm_id:
                    if current_hadm_id != "":
                        for current_icd_code_and_flag in current_icd_codes_and_flags:
                            current_icd_code = current_icd_code_and_flag[0]
                            current_icd_flag = current_icd_code_and_flag[1]
                            if current_icd_flag == 9:
                                cur.execute("INSERT INTO patients_icd9_codes (subject_id, hadm_id, icd9_code) VALUES (?, ?, ?);", 
                                            (current_subject_id, current_hadm_id, current_icd_code))
                            elif current_icd_flag == 10:
                                cur2.execute("INSERT INTO patients_icd10_codes (subject_id, hadm_id, icd10_code) VALUES (?, ?, ?);", 
                                            (current_subject_id, current_hadm_id, current_icd_code))
                    current_subject_id = subject_id
                    current_hadm_id = hadm_id
                    current_icd_codes_and_flags = []
                current_icd_code_and_flag = [icd_code, flag]
                if current_icd_code_and_flag not in current_icd_codes_and_flags:
                    current_icd_codes_and_flags.append([icd_code, flag])
            current_line_num += 1
    for current_icd_code_and_flag in current_icd_codes_and_flags:
        current_icd_code = current_icd_code_and_flag[0]
        current_icd_flag = current_icd_code_and_flag[1]
        if current_icd_flag == 9:
            cur.execute("INSERT INTO patients_icd9_codes (subject_id, hadm_id, icd9_code) VALUES (?, ?, ?);", 
                        (current_subject_id, current_hadm_id, current_icd_code))
        elif current_icd_flag == 10:
            cur2.execute("INSERT INTO patients_icd10_codes (subject_id, hadm_id, icd10_code) VALUES (?, ?, ?);", 
                        (current_subject_id, current_hadm_id, current_icd_code))
    con.commit()
    con2.commit()
    cur.execute("SELECT * FROM patients_icd9_codes;")
    for row in cur.fetchall():
        print(row)
    cur2.execute("SELECT * FROM patients_icd10_codes;")
    for row in cur2.fetchall():
        print(row)
    con.commit()
    con.close()

def icd_to_phecodes():
    con = sqlite3.connect('icd_to_phecodes.db')
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS icd_to_phecodes")
    cur.execute(f"CREATE TABLE icd_to_phecodes (ICD, flag, phecode);")
    with open('ICD-CM to phecode, unrolled.txt') as file:
        amt_of_icd_codes = len(file.readlines())
    with open('ICD-CM to phecode, unrolled.txt') as file:
        current_line_num = 1
        while current_line_num <= amt_of_icd_codes:
            current_line = file.readline().split('\t')
            print(current_line)
            if current_line_num > 1:
                icd_code_split = current_line[0].split('.')
                ICD = ""
                for icd_code_piece in icd_code_split:
                    ICD += icd_code_piece
                flag = int(current_line[1])
                phecode = current_line[2].split('\n')[0]
                cur.execute("INSERT INTO icd_to_phecodes (ICD, flag, phecode) VALUES (?, ?, ?);", 
                            (ICD, flag, phecode))
            current_line_num += 1
    cur.execute("SELECT * FROM icd_to_phecodes;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def icd_to_phecodes_data_structures():
    con = sqlite3.connect('icd_to_phecodes.db')
    cur = con.cursor()
    cur.execute("SELECT * FROM icd_to_phecodes;")
    icd9_to_phecodes = {}
    icd10_to_phecodes = {}
    for row in cur.fetchall():
        ICD = row[0]
        flag = row[1]
        phecode = row[2]
        if flag == 9:
            if ICD not in icd9_to_phecodes:
                icd9_to_phecodes[ICD] = set()
            icd9_to_phecodes[ICD].add(phecode)
        elif flag == 10:
            if ICD not in icd10_to_phecodes:
                icd10_to_phecodes[ICD] = set()
            icd10_to_phecodes[ICD].add(phecode)
        print(f"Phecode: {phecode}")
    return icd9_to_phecodes, icd10_to_phecodes

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
    icd9_to_phecodes, icd10_to_phecodes = icd_to_phecodes_data_structures()
    hadm_id_to_dischtimes = hadm_id_to_dischtimes_data_structure()
    cur = con.cursor()
    con3 = sqlite3.connect("patients_icd9_codes.db")
    cur3 = con3.cursor()
    cur3.execute("SELECT * FROM patients_icd9_codes;")
    for row in cur3.fetchall():
        print(f"A row in cur3.fetchall is {row}")
    cur3 = con3.cursor()
    cur3.execute("SELECT * FROM patients_icd9_codes;")
    con4 = sqlite3.connect("patients_icd10_codes.db")
    cur4 = con4.cursor()
    cur4.execute("SELECT * FROM patients_icd10_codes;")
    for row in cur4.fetchall():
        print(f"A row in cur4.fetchall is {row}")
    cur4 = con4.cursor()
    cur4.execute("SELECT * FROM patients_icd10_codes;")
    subject_id_to_phecodes_dischtimes = {}
    for row in cur3.fetchall():
        print(f"cur3 row: {row}")
        current_subject_id = row[0]
        hadm_id = row[1]
        icd9_code = row[2]
        if icd9_code in icd9_to_phecodes:
            current_phecodes = icd9_to_phecodes[icd9_code]
            for phecode in current_phecodes:
                dischtime = hadm_id_to_dischtimes[hadm_id]
                if phecode in phecodes:
                    if current_subject_id not in subject_id_to_phecodes_dischtimes.keys():
                        subject_id_to_phecodes_dischtimes[current_subject_id] = set()
                    phecode_is_in_tuple_already = False
                    for current in subject_id_to_phecodes_dischtimes[current_subject_id]:
                        if phecode == current[0]:
                            phecode_is_in_tuple_already = True
                    if not phecode_is_in_tuple_already:
                        subject_id_to_phecodes_dischtimes[current_subject_id].add(tuple([phecode, dischtime]))
                        print("ICD9:")
                        print(tuple([phecode, dischtime]))
    for row in cur4.fetchall():
        print(f"cur4 row: {row}")
        current_subject_id = row[0]
        hadm_id = row[1]
        icd10_code = row[2]
        if icd10_code in icd10_to_phecodes:
            current_phecodes = icd10_to_phecodes[icd10_code]
            for phecode in current_phecodes:
                dischtime = hadm_id_to_dischtimes[hadm_id]
                if phecode in phecodes:
                    if current_subject_id not in subject_id_to_phecodes_dischtimes.keys():
                        subject_id_to_phecodes_dischtimes[current_subject_id] = set()
                    phecode_is_in_tuple_already = False
                    for current in subject_id_to_phecodes_dischtimes[current_subject_id]:
                        if phecode == current[0]:
                            phecode_is_in_tuple_already = True
                    if not phecode_is_in_tuple_already:
                        subject_id_to_phecodes_dischtimes[current_subject_id].add(tuple([phecode, dischtime]))
                        print("ICD10:")
                        print(tuple([phecode, dischtime]))
    # Fill in each row with dischtime for each phecode the row's patient has
    for current_subject_id in subject_id_to_phecodes_dischtimes.keys():
        current_phecodes_dischtimes = subject_id_to_phecodes_dischtimes[current_subject_id]
        current_update_phrase = ""
        for current_phecode_dischtime in current_phecodes_dischtimes:
            phecode = current_phecode_dischtime[0]
            dischtime = current_phecode_dischtime[1]
            current_update_phrase += f"`{phecode}` = '{dischtime}', "
        if current_update_phrase != "":
            current_update_phrase = current_update_phrase.removesuffix(", ")
            cur = con.cursor()
            cur.execute(f"UPDATE patients_phecodes_dischtimes SET {current_update_phrase} WHERE subject_id = '{current_subject_id}';")
            con.commit()
            print(f"UPDATE patients_phecodes_dischtimes SET {current_update_phrase} WHERE subject_id = '{current_subject_id}';")
        print(f"current_subject_id: {current_subject_id}")
    con.commit()
    con2.commit()
    con3.commit()
    con2.close()
    con3.close()
    cur = con.cursor()
    cur.execute("SELECT * FROM patients_phecodes_dischtimes")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def create_X_and_y_pancreatic_cancer_sql():
    print("Starting create_X_and_y_pancreatic_cancer_sql")
    con = sqlite3.connect("X_and_y_pancreatic_cancer.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS X_and_y_pancreatic_cancer;")
    # diagnosed_157 is y
    # everything after diagnosed_157 is X
    cur.execute("CREATE TABLE X_and_y_pancreatic_cancer (diagnosed_157, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height, marital_status, black, white, male, anchor_age, `174`, `174.1`, `174.2`, `250.2`, `250.21`, `250.22`, `250.23`, `250.24`, `577.2`);")
    black_categories = ["BLACK/AFRICAN", "BLACK/AFRICAN AMERICAN", "BLACK/CAPE VERDEAN", "BLACK/CARIBBEAN ISLAND"]
    white_categories = ["WHITE", "WHITE - BRAZILIAN", "WHITE - EASTERN EUROPEAN", "WHITE - OTHER EUROPEAN", "WHITE - RUSSIAN"]
    con2 = sqlite3.connect("admitted_patients.db")
    cur2 = con2.cursor()
    cur2.execute("SELECT * FROM admitted_patients;")
    ## csv_to_sql_hosp_omr_summary(157)
    con3 = sqlite3.connect("omr_summary.db")
    cur3 = con3.cursor()
    con4 = sqlite3.connect("patients_phecodes_dischtimes.db")
    cur4 = con4.cursor()
    con5 = sqlite3.connect("patients_gender_and_anchor_age.db")
    cur5 = con5.cursor()
    batch_data = []
    avg_systolic_no_diagnosis = []
    avg_systolic_yes_diagnosis = []
    avg_diastolic_no_diagnosis = []
    avg_diastolic_yes_diagnosis = []
    avg_weight_no_diagnosis = []
    avg_weight_yes_diagnosis = []
    avg_bmi_no_diagnosis = []
    avg_bmi_yes_diagnosis = []
    avg_height_no_diagnosis = []
    avg_height_yes_diagnosis = []
    for row in cur2.fetchall():
        subject_id = row[0]
        marital_status = 1 if row[1] == "MARRIED" else 0
        race = row[2]
        black = 1 if race in black_categories else 0
        white = 1 if race in white_categories else 0
        cur3.execute("SELECT avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height FROM omr_summary WHERE subject_id = ?;", (subject_id,))
        omr_summary_row = cur3.fetchone()
        print(omr_summary_row)
        cur5.execute("SELECT gender, anchor_age FROM patients_gender_and_anchor_age WHERE subject_id = ?;", (subject_id,))
        gender_and_anchor_age_row = cur5.fetchone()
        print(subject_id)
        print(gender_and_anchor_age_row)
        if gender_and_anchor_age_row is not None and omr_summary_row is not None:
            male = 1 if gender_and_anchor_age_row[0] == "M" else 0
            anchor_age = gender_and_anchor_age_row[1]
            avg_systolic = omr_summary_row[0]
            avg_diastolic = omr_summary_row[1]
            avg_weight = omr_summary_row[2]
            avg_bmi = omr_summary_row[3]
            avg_height = omr_summary_row[4]
            if avg_systolic != "N/A" and avg_diastolic != "N/A" and avg_weight != "N/A" and avg_bmi != "N/A" and avg_height != "N/A":
                cur4.execute(f"SELECT `157`, `174`, `174.1`, `174.2`, `250.2`, `250.21`, `250.22`, `250.23`, `250.24`, `577.2` FROM patients_phecodes_dischtimes WHERE subject_id = '{subject_id}';")
                diagnosed_157_time, diagnosed_174_time, diagnosed_174_1_time, diagnosed_174_2_time, diagnosed_250_2_time, diagnosed_250_21_time, diagnosed_250_22_time, diagnosed_250_23_time, diagnosed_250_24_time, diagnosed_577_2_time = cur4.fetchone()
                diagnosed_157 = 0 if diagnosed_157_time == 0 else 1
                diagnosed_174 = 1 if diagnosed_174_time != 0 and (diagnosed_157_time == 0 or diagnosed_174_time < diagnosed_157_time) else 0
                diagnosed_174_1 = 1 if diagnosed_174_1_time != 0 and (diagnosed_157_time == 0 or diagnosed_174_1_time < diagnosed_157_time) else 0
                diagnosed_174_2 = 1 if diagnosed_174_2_time != 0 and (diagnosed_157_time == 0 or diagnosed_174_2_time < diagnosed_157_time) else 0
                diagnosed_250_2 = 1 if diagnosed_250_2_time != 0 and (diagnosed_157_time == 0 or diagnosed_250_2_time < diagnosed_157_time) else 0
                diagnosed_250_21 = 1 if diagnosed_250_21_time != 0 and (diagnosed_157_time == 0 or diagnosed_250_21_time < diagnosed_157_time) else 0
                diagnosed_250_22 = 1 if diagnosed_250_22_time != 0 and (diagnosed_157_time == 0 or diagnosed_250_22_time < diagnosed_157_time) else 0
                diagnosed_250_23 = 1 if diagnosed_250_23_time != 0 and (diagnosed_157_time == 0 or diagnosed_250_23_time < diagnosed_157_time) else 0
                diagnosed_250_24 = 1 if diagnosed_250_24_time != 0 and (diagnosed_157_time == 0 or diagnosed_250_24_time < diagnosed_157_time) else 0
                diagnosed_577_2 = 1 if diagnosed_577_2_time != 0 and (diagnosed_157_time == 0 or diagnosed_577_2_time < diagnosed_157_time) else 0
                batch_data.append([diagnosed_157, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height, marital_status, black, white, male, anchor_age, diagnosed_174, diagnosed_174_1, diagnosed_174_2, diagnosed_250_2, diagnosed_250_21, diagnosed_250_22, diagnosed_250_23, diagnosed_250_24, diagnosed_577_2])
                avg_systolic_no_diagnosis.append(avg_systolic) if diagnosed_157 == 0 else avg_systolic_yes_diagnosis.append(avg_systolic)
                avg_diastolic_no_diagnosis.append(avg_diastolic) if diagnosed_157 == 0 else avg_diastolic_yes_diagnosis.append(avg_diastolic)
                avg_weight_no_diagnosis.append(avg_weight) if diagnosed_157 == 0 else avg_weight_yes_diagnosis.append(avg_weight)
                avg_bmi_no_diagnosis.append(avg_bmi) if diagnosed_157 == 0 else avg_bmi_yes_diagnosis.append(avg_bmi)
                avg_height_no_diagnosis.append(avg_height) if diagnosed_157 == 0 else avg_height_yes_diagnosis.append(avg_height)
                print(f"NOT YET: INSERT INTO X_and_y_pancreatic_cancer (diagnosed_157, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height, marital_status, black, white, male, anchor_age, `174`, `174.1`, `174.2`, `250.2`, `250.21`, `250.22`, `250.23`, `250.24`, `577.2`) VALUES ({diagnosed_157}, {avg_systolic}, {avg_diastolic}, {avg_weight}, {avg_bmi}, {avg_height}, {marital_status}, {black}, {white}, {male}, {anchor_age}, {diagnosed_174}, {diagnosed_174_1}, {diagnosed_174_2}, {diagnosed_250_2}, {diagnosed_250_21}, {diagnosed_250_22}, {diagnosed_250_23}, {diagnosed_250_24}, {diagnosed_577_2});")
                if len(batch_data) > 500:
                    cur.executemany("INSERT INTO X_and_y_pancreatic_cancer (diagnosed_157, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height, marital_status, black, white, male, anchor_age, `174`, `174.1`, `174.2`, `250.2`, `250.21`, `250.22`, `250.23`, `250.24`, `577.2`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    batch_data)
                    batch_data = []
    cur.executemany("INSERT INTO X_and_y_pancreatic_cancer (diagnosed_157, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height, marital_status, black, white, male, anchor_age, `174`, `174.1`, `174.2`, `250.2`, `250.21`, `250.22`, `250.23`, `250.24`, `577.2`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    batch_data)
    con.commit()
    con2.commit()
    con3.commit()
    con4.commit()
    con.close()
    con2.close()
    con3.close()
    con4.close()
    avg_systolic_overall = avg_systolic_no_diagnosis + avg_systolic_yes_diagnosis
    avg_diastolic_overall = avg_diastolic_no_diagnosis + avg_diastolic_yes_diagnosis
    avg_weight_overall = avg_weight_no_diagnosis + avg_weight_yes_diagnosis
    avg_bmi_overall = avg_bmi_no_diagnosis + avg_bmi_yes_diagnosis
    avg_height_overall = avg_height_no_diagnosis + avg_height_yes_diagnosis
    print(f"Length of avg_systolic_overall: {len(avg_systolic_overall)}")
    print(f"Mean of avg_systolic_overall: {np.mean(avg_systolic_overall)}")
    print(f"Standard deviation of avg_systolic_overall: {np.std(avg_systolic_overall)}")
    print(f"Length of avg_diastolic_overall: {len(avg_diastolic_overall)}")
    print(f"Mean of avg_diastolic_overall: {np.mean(avg_diastolic_overall)}")
    print(f"Standard deviation of avg_diastolic_overall: {np.std(avg_diastolic_overall)}")
    print(f"Length of avg_weight_overall: {len(avg_weight_overall)}")
    print(f"Mean of avg_weight_overall: {np.mean(avg_weight_overall)}")
    print(f"Standard deviation of avg_weight_overall: {np.std(avg_weight_overall)}")
    print(f"Length of avg_bmi_overall: {len(avg_bmi_overall)}")
    print(f"Mean of avg_bmi_overall: {np.mean(avg_bmi_overall)}")
    print(f"Standard deviation of avg_bmi_overall: {np.std(avg_bmi_overall)}")
    print(f"Length of avg_height_overall: {len(avg_height_overall)}")
    print(f"Mean of avg_height_overall: {np.mean(avg_height_overall)}")
    print(f"Standard deviation of avg_height_overall: {np.std(avg_height_overall)}")
    print(f"----------")
    print(f"Length of avg_systolic_no_diagnosis: {len(avg_systolic_no_diagnosis)}")
    print(f"Mean of avg_systolic_no_diagnosis: {np.mean(avg_systolic_no_diagnosis)}")
    print(f"Standard deviation of avg_systolic_no_diagnosis: {np.std(avg_systolic_no_diagnosis)}")
    print(f"Length of avg_diastolic_no_diagnosis: {len(avg_diastolic_no_diagnosis)}")
    print(f"Mean of avg_diastolic_no_diagnosis: {np.mean(avg_diastolic_no_diagnosis)}")
    print(f"Standard deviation of avg_diastolic_no_diagnosis: {np.std(avg_diastolic_no_diagnosis)}")
    print(f"Length of avg_weight_no_diagnosis: {len(avg_weight_no_diagnosis)}")
    print(f"Mean of avg_weight_no_diagnosis: {np.mean(avg_weight_no_diagnosis)}")
    print(f"Standard deviation of avg_weight_no_diagnosis: {np.std(avg_weight_no_diagnosis)}")
    print(f"Length of avg_bmi_no_diagnosis: {len(avg_bmi_no_diagnosis)}")
    print(f"Mean of avg_bmi_no_diagnosis: {np.mean(avg_bmi_no_diagnosis)}")
    print(f"Standard deviation of avg_bmi_no_diagnosis: {np.std(avg_bmi_no_diagnosis)}")
    print(f"Length of avg_height_no_diagnosis: {len(avg_height_no_diagnosis)}")
    print(f"Mean of avg_height_no_diagnosis: {np.mean(avg_height_no_diagnosis)}")
    print(f"Standard deviation of avg_height_no_diagnosis: {np.std(avg_height_no_diagnosis)}")
    print(f"----------")
    print(f"Length of avg_systolic_yes_diagnosis: {len(avg_systolic_yes_diagnosis)}")
    print(f"Mean of avg_systolic_yes_diagnosis: {np.mean(avg_systolic_yes_diagnosis)}")
    print(f"Standard deviation of avg_systolic_yes_diagnosis: {np.std(avg_systolic_yes_diagnosis)}")
    print(f"Length of avg_diastolic_yes_diagnosis: {len(avg_diastolic_yes_diagnosis)}")
    print(f"Mean of avg_diastolic_yes_diagnosis: {np.mean(avg_diastolic_yes_diagnosis)}")
    print(f"Standard deviation of avg_diastolic_yes_diagnosis: {np.std(avg_diastolic_yes_diagnosis)}")
    print(f"Length of avg_weight_yes_diagnosis: {len(avg_weight_yes_diagnosis)}")
    print(f"Mean of avg_weight_yes_diagnosis: {np.mean(avg_weight_yes_diagnosis)}")
    print(f"Standard deviation of avg_weight_yes_diagnosis: {np.std(avg_weight_yes_diagnosis)}")
    print(f"Length of avg_bmi_yes_diagnosis: {len(avg_bmi_yes_diagnosis)}")
    print(f"Mean of avg_bmi_yes_diagnosis: {np.mean(avg_bmi_yes_diagnosis)}")
    print(f"Standard deviation of avg_bmi_yes_diagnosis: {np.std(avg_bmi_yes_diagnosis)}")
    print(f"Length of avg_height_yes_diagnosis: {len(avg_height_yes_diagnosis)}")
    print(f"Mean of avg_height_yes_diagnosis: {np.mean(avg_height_yes_diagnosis)}")
    print(f"Standard deviation of avg_height_yes_diagnosis: {np.std(avg_height_yes_diagnosis)}")
    print("Ending create_X_and_y_pancreatic_cancer_sql")

def logistic_regression(X_and_y_database_name):
    X = []
    y = []
    con = sqlite3.connect(f"{X_and_y_database_name}.db")
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {X_and_y_database_name};")
    for row in cur.fetchall():
        X.append(row[1:])
        y.append(row[0])
    con.commit()
    con.close()
    print(f"X length: {len(X)}")
    print(f"First few X: {X[0:20]}")
    print(f"y length: {len(y)}")
    print(f"First few y: {y[0:20]}")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1)
    logreg = linear_model.LogisticRegression(class_weight='balanced', max_iter=10000)
    logreg.fit(X_train, y_train)
    y_pred = logreg.predict(X_test)
    cnf_matrix = metrics.confusion_matrix(y_test, y_pred, labels=[0, 1])
    print(cnf_matrix)
    print(f"Predicted labels length: {len(y_pred)}")
    print(f"First few predicted labels: {y_pred[0:20]}")
    print(f"Accuracy: {metrics.accuracy_score(y_test, y_pred)}")
    print(f"Precision: {metrics.precision_score(y_test, y_pred)}")
    print(f"Recall: {metrics.recall_score(y_test, y_pred)}")
    print(f"F1-Score: {metrics.f1_score(y_test, y_pred)}")

def svm(X_and_y_database_name):
    X = []
    y = []
    con = sqlite3.connect(f"{X_and_y_database_name}.db")
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {X_and_y_database_name};")
    for row in cur.fetchall():
        X.append(row[1:])
        y.append(row[0])
    con.commit()
    con.close()
    print(f"X length: {len(X)}")
    print(f"First few X: {X[0:20]}")
    print(f"y length: {len(y)}")
    print(f"First few y: {y[0:20]}")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1)
    # clf = svm_callee.LinearSVC(class_weight='balanced')
    clf = svm_callee.SVC(class_weight='balanced')
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)
    cnf_matrix = metrics.confusion_matrix(y_test, y_pred, labels=[0, 1])
    print(cnf_matrix)
    print(f"Predicted labels length: {len(y_pred)}")
    print(f"First few predicted labels: {y_pred[0:20]}")
    print(f"Accuracy: {metrics.accuracy_score(y_test, y_pred)}")
    print(f"Precision: {metrics.precision_score(y_test, y_pred)}")
    print(f"Recall: {metrics.recall_score(y_test, y_pred)}")
    print(f"F1-Score: {metrics.f1_score(y_test, y_pred)}")

# # csv_to_sql_hosp_drgcodes()
# # csv_to_sql_hosp_d_icd_diagnoses()
# patients_icd_codes()
# icd_to_phecodes()
# # csv_to_sql_hosp_admissions()
# # csv_to_sql_hosp_omr()
# patients_gender_and_anchor_age()
# admitted_patients()
# calculate_demographics()
# csv_to_sql_hosp_omr_summary()
# patients_phecodes_dischtimes_sql_hosp()

# create_X_and_y_pancreatic_cancer_sql()

# logistic_regression("X_and_y_pancreatic_cancer")

svm("X_and_y_pancreatic_cancer")
