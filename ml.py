import numpy as np
import sqlite3
import time
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn import svm
from sklearn import metrics
import csv

def calculate_demographics():
    gender_list = []
    anchor_age_list = []
    with open('mimic-iv-2.2/hosp/patients.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iv-2.2/hosp/patients.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                gender_list.append(current_line[1])
                anchor_age_list.append(int(current_line[2]))
            current_line_num += 1
    gender_counts = np.unique(gender_list, return_counts=True)
    num_f = gender_counts[1][0]
    num_m = gender_counts[1][1]
    print(f"Number of 'F': {num_f}")
    print(f"Proportion of 'F': {num_f / (num_f + num_m)}")
    print(f"Number of 'M': {num_m}")
    print(f"Proportion of 'M': {num_m / (num_f + num_m)}")
    print(f"Mean anchor age: {np.mean(anchor_age_list)}")
    print(f"Standard deviation anchor age: {np.std(anchor_age_list)}")
    con = sqlite3.connect("admitted_patients.db")
    cur = con.cursor()
    cur.execute("SELECT race FROM admitted_patients;")
    race_list = []
    for row in cur.fetchall():
        race_list.append(row[0])
    race_counts = np.unique(race_list, return_counts=True)
    print(race_counts)
    num_people = np.sum(race_counts[1])
    names_of_race_categories = ["AMERICAN INDIAN/ALASKA NATIVE", "ASIAN", "ASIAN - ASIAN INDIAN", "ASIAN - CHINESE", "ASIAN - KOREAN", 
                                "ASIAN - SOUTH EAST ASIAN", "BLACK/AFRICAN", "BLACK/AFRICAN AMERICAN", "BLACK/CAPE VERDEAN", 
                                "BLACK/CARRIBEAN ISLAND", "HISPANIC OR LATINO", "HISPANIC/LATINO - CENTRAL AMERICAN", "HISPANIC/LATINO - COLUMBIAN", 
                                "HISPANIC/LATINO - CUBAN", "HISPANIC/LATINO - DOMINICAN", "HISPANIC/LATINO - GUATEMALAN", "HISPANIC/LATINO - HONDURAN", 
                                "HISPANIC/LATINO - MEXICAN", "HISPANIC/LATINO - PUERTO RICAN", "HISPANIC/LATINO - SALVADORAN", "MULTIPLE RACE/ETHNICITY", 
                                "NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER", "OTHER", "PATIENT DECLINED TO ANSWER", "PORTUGUESE", "SOUTH AMERICAN", 
                                "UNABLE TO OBTAIN", "UNKNOWN", "WHITE", "WHITE - BRAZILIAN", "WHITE - EASTERN EUROPEAN", "WHITE - OTHER EUROPEAN", 
                                "WHITE - RUSSIAN"]
    for i in range(len(race_counts[1])):
        print(f"Number of {names_of_race_categories[i]}: {race_counts[1][i]}")
        print(f"Proportion of {names_of_race_categories[i]}: {race_counts[1][i] / num_people}")


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
    con = sqlite3.connect("omr_summary.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS omr_summary;")
    cur.execute("CREATE TABLE omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height);")
    con2 = sqlite3.connect("patients_phecodes_dischtimes.db")
    cur2 = con2.cursor()
    con3 = sqlite3.connect("admitted_patients.db")
    cur3 = con3.cursor()
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
        amt_height = 0 # ASK IF IT'S IN ADMITTED_PATIENTS AND FILTER OUT IF NOT
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
                    cur2 = con2.cursor()
                    print(f"'{new_subject_id}'")
                    cur2.execute(f"SELECT `{phecode}` FROM patients_phecodes_dischtimes WHERE subject_id = '{new_subject_id}';")
                    read_line = cur2.fetchone()
                    if read_line is None:
                        current_line_num += 1
                        continue
                    dischtime = read_line[0]
                    if dischtime != 0:
                        dischtime = datetime.strptime(dischtime, '%Y-%m-%d %H:%M:%S')
                if new_subject_id != subject_id:
                    # If we have moved onto a new subject, put the averages from the old subject into omr_summary
                    if subject_id != "":
                        avg_systolic = "N/A" if amt_blood_pressure == 0 else round(total_systolic / amt_blood_pressure)
                        avg_diastolic = "N/A" if amt_blood_pressure == 0 else round(total_diastolic / amt_blood_pressure)
                        avg_weight = "N/A" if amt_weight == 0 else round(total_weight / amt_weight, 2)
                        avg_bmi = "N/A" if amt_bmi == 0 else round(total_bmi / amt_bmi, 1)
                        avg_height = "N/A" if amt_height == 0 else round(total_height / amt_height, 2)
                        print(f"INSERT INTO omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES ({subject_id}, {avg_systolic}, {avg_diastolic}, {avg_weight}, {avg_bmi}, {avg_height});")
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
                con.commit()
            current_line_num += 1
    # Put the averages from the final patient into omr_summary
    avg_systolic = "N/A" if amt_blood_pressure == 0 else round(total_systolic / amt_blood_pressure)
    avg_diastolic = "N/A" if amt_blood_pressure == 0 else round(total_diastolic / amt_blood_pressure)
    avg_weight = "N/A" if amt_weight == 0 else round(total_weight / amt_weight, 2)
    avg_bmi = "N/A" if amt_bmi == 0 else round(total_bmi / amt_bmi, 1)
    avg_height = "N/A" if amt_height == 0 else round(total_height / amt_height, 2)
    print(f"INSERT INTO omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES ({subject_id}, {avg_systolic}, {avg_diastolic}, {avg_weight}, {avg_bmi}, {avg_height});")
    cur.execute("INSERT INTO omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES (?, ?, ?, ?, ?, ?);", 
                (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height))
    cur = con.cursor()
    cur.execute("SELECT * FROM omr_summary;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    cur = con.cursor()
    con2 = sqlite3.connect("admitted_patients.db")
    cur2 = con2.cursor()
    cur2.execute("SELECT subject_id FROM admitted_patients;")
    not_in_omr_summary = []
    for row in cur2.fetchall():
        subject_id = row[0]
        print(subject_id)
        cur.execute("SELECT * FROM omr_summary WHERE subject_id = ?;", (subject_id,))
        if len(cur.fetchall()) == 0:
            not_in_omr_summary.append(subject_id)
    for subject_id in not_in_omr_summary:
        cur.execute("INSERT INTO omr_summary (subject_id, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height) VALUES (?, ?, ?, ?, ?, ?);",
                    (subject_id, "N/A", "N/A", "N/A", "N/A", "N/A"))
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
    time.sleep(10)
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
    time.sleep(10)
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
    # diagnosed_156 is y
    # everything after diagnosed_157 is X
    cur.execute("CREATE TABLE X_and_y_pancreatic_cancer (subject_id, diagnosed_157, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height, marital_status, american_indian_alaska_native, asian, black, hispanic_latino, multiple_race_ethnicity, native_hawaiian_other_pacific_islander, other_declined_unable_unknown, portuguese, south_american, white, `174`, `174.1`, `174.2`, `250.2`, `250.21`, `250.22`, `250.23`, `250.24`, `577.2`);")
    american_indian_alaska_native_categories = ["AMERICAN INDIAN/ALASKA NATIVE"]
    asian_categories = ["ASIAN", "ASIAN - ASIAN INDIAN", "ASIAN - CHINESE", "ASIAN - KOREAN", "ASIAN - SOUTH EAST ASIAN"]
    black_categories = ["BLACK/AFRICAN", "BLACK/AFRICAN AMERICAN", "BLACK/CAPE VERDEAN", "BLACK/CARRIBEAN ISLAND"]
    hispanic_latino_categories = ["HISPANIC OR LATINO", "HISPANIC/LATINO - CENTRAL AMERICAN", "HISPANIC/LATINO - COLUMBIAN", 
                                "HISPANIC/LATINO - CUBAN", "HISPANIC/LATINO - DOMINICAN", "HISPANIC/LATINO - GUATEMALAN", 
                                "HISPANIC/LATINO - HONDURAN", "HISPANIC/LATINO - MEXICAN", "HISPANIC/LATINO - PUERTO RICAN", 
                                "HISPANIC/LATINO - SALVADORAN"]
    multiple_race_ethnicity_categories = ["MULTIPLE RACE/ETHNICITY"]
    native_hawaiian_other_pacific_islander_categories = ["NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER"]
    other_declined_unable_unknown_categories = ["OTHER", "PATIENT DECLINED TO ANSWER", "UNABLE TO OBTAIN", "UNKNOWN"]
    portuguese_categories = ["PORTUGUESE"]
    south_american_categories = ["SOUTH AMERICAN"]
    white_categories = ["WHITE", "WHITE - BRAZILIAN", "WHITE - EASTERN EUROPEAN", "WHITE - OTHER EUROPEAN", "WHITE - RUSSIAN"]
    con2 = sqlite3.connect("admitted_patients.db")
    cur2 = con2.cursor()
    cur2.execute("SELECT * FROM admitted_patients;")
    csv_to_sql_hosp_omr_summary(157)
    con3 = sqlite3.connect("omr_summary.db")
    cur3 = con3.cursor()
    con4 = sqlite3.connect("patients_phecodes_dischtimes.db")
    cur4 = con4.cursor()
    for row in cur2.fetchall():
        subject_id = row[0]
        marital_status = row[1]
        race = row[2]
        american_indian_alaska_native = 1 if race in american_indian_alaska_native_categories else 0
        asian = 1 if race in asian_categories else 0
        black = 1 if race in black_categories else 0
        hispanic_latino = 1 if race in hispanic_latino_categories else 0
        multiple_race_ethnicity = 1 if race in multiple_race_ethnicity_categories else 0
        native_hawaiian_other_pacific_islander = 1 if race in native_hawaiian_other_pacific_islander_categories else 0
        other_declined_unable_unknown = 1 if race in other_declined_unable_unknown_categories else 0
        portuguese = 1 if race in portuguese_categories else 0
        south_american = 1 if race in south_american_categories else 0
        white = 1 if race in white_categories else 0
        cur3 = con3.cursor()
        cur3.execute("SELECT * FROM omr_summary WHERE subject_id = ?;", (subject_id,))
        omr_summary_row = cur3.fetchone()
        print(omr_summary_row)
        avg_systolic = omr_summary_row[0]
        avg_diastolic = omr_summary_row[1]
        avg_weight = omr_summary_row[2]
        avg_bmi = omr_summary_row[3]
        avg_height = omr_summary_row[4]
        cur4.execute(f"SELECT (`157`, `174`, `174.1`, `174.2`, `250.2`, `250.21`, `250.22`, `250.23`, `250.24`, `577.2`) FROM patients_phecodes_dischtimes WHERE subject_id = '{subject_id}';")
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
        cur.execute("INSERT INTO X_and_y_pancreatic_cancer (subject_id, diagnosed_157, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height, marital_status, american_indian_alaska_native, asian, black, hispanic_latino, multiple_race_ethnicity, native_hawaiian_other_pacific_islander, other_declined_unable_unknown, portuguese, south_american, white, `174`, `174.1`, `174.2`, `250.2`, `250.21`, `250.22`, `250.23`, `250.24`, `577.2`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (subject_id, diagnosed_157, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height, marital_status, american_indian_alaska_native, asian, black, hispanic_latino, multiple_race_ethnicity, native_hawaiian_other_pacific_islander, other_declined_unable_unknown, portuguese, south_american, white, diagnosed_174, diagnosed_174_1, diagnosed_174_2, diagnosed_250_2, diagnosed_250_21, diagnosed_250_22, diagnosed_250_23, diagnosed_250_24, diagnosed_577_2))
        con.commit()
    con.commit()
    con2.commit()
    con3.commit()
    con4.commit()
    con.close()
    con2.close()
    con3.close()
    con4.close()
    print("Ending create_X_and_y_pancreatic_cancer_sql")

def run_svm(X_and_y_database_name):
    print("Starting run_svm")
    X = []
    y = []
    con = sqlite3.connect(f"{X_and_y_database_name}.db")
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {X_and_y_database_name};")
    for row in cur.fetchall():
        X_entry = row[2:]
        if "N/A" not in X_entry and "" not in X_entry:
            X.append(X_entry)
            y_entry = row[1]
            y.append(y_entry)
            con.commit()
    con.commit()
    con.close()
    print(f"X length: {len(X)}")
    print(f"First few X: {X[0:20]}")
    print(f"y length: {len(y)}")
    print(f"All of the y: {y}")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    clf = svm.SVC(kernel='linear')
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)
    print(f"Predicted labels length: {len(y_pred)}")
    print(f"First few predicted labels: {y_pred[0:20]}")
    print(f"Accuracy: {metrics.accuracy_score(y_test, y_pred)}")
    print(f"Precision: {metrics.precision_score(y_test, y_pred)}")
    print(f"Recall: {metrics.recall_score(y_test, y_pred)}")
    print("Ending run_svm")

def create_csv_of_llm_prompts_pancreatic_cancer():
    con = sqlite3.connect("X_and_y_pancreatic_cancer.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM X_and_y_pancreatic_cancer;")
    fields = ["text"]
    rows = []
    filename = "llm_prompts.csv"
    for row in cur.fetchall():
        _, diagnosed_157, avg_systolic, avg_diastolic, avg_weight, avg_bmi, avg_height, marital_status, american_indian_alaska_native, asian, black, hispanic_latino, multiple_race_ethnicity, native_hawaiian_other_pacific_islander, other_declined_unable_unknown, portuguese, south_american, white, diagnosed_174, diagnosed_174_1, diagnosed_174_2, diagnosed_250_2, diagnosed_250_21, diagnosed_250_22, diagnosed_250_23, diagnosed_250_24, diagnosed_577_2 = row
        race = "AMERICAN INDIAN/ALASKA NATIVE" if american_indian_alaska_native == 1 else ""
        race = "ASIAN" if asian == 1 else race
        race = "BLACK" if black == 1 else race
        race = "HISPANIC OR LATINO" if hispanic_latino == 1 else race
        race = "MULTIPLE RACE/ETHNICITY" if multiple_race_ethnicity == 1 else race
        race = "NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER" if native_hawaiian_other_pacific_islander == 1 else race
        race = "OTHER, PATIENT DECLINED TO ANSWER, UNABLE TO OBTAIN, OR UNKNOWN" if other_declined_unable_unknown == 1 else race
        race = "PORTUGUESE" if portuguese == 1 else race
        race = "SOUTH AMERICAN" if south_american == 1 else race
        race = "WHITE" if white == 1 else race
        pancreatic_cancer = "Diagnosed" if diagnosed_157 == 1 else "Not diagnosed"
        breast_cancer = "Diagnosed" if diagnosed_174 == 1 else "Not diagnosed"
        breast_cancer_female = "Diagnosed" if diagnosed_174_1 == 1 else "Not diagnosed"
        breast_cancer_male = "Diagnosed" if diagnosed_174_2 == 1 else "Not diagnosed"
        type_2_diabetes = "Diagnosed" if diagnosed_250_2 == 1 else "Not diagnosed"
        type_2_diabetes_with_ketoacidosis = "Diagnosed" if diagnosed_250_21 == 1 else "Not diagnosed"
        type_2_diabetes_with_renal_manifestations = "Diagnosed" if diagnosed_250_22 == 1 else "Not diagnosed"
        type_2_diabetes_with_opthalmic_manifestations = "Diagnosed" if diagnosed_250_23 == 1 else "Not diagnosed"
        type_2_diabetes_with_neurological_manifestations = "Diagnosed" if diagnosed_250_24 == 1 else "Not diagnosed"
        chronic_pancreatitis = "Diagnosed" if diagnosed_577_2 == 1 else "Not diagnosed"
        prompt = f"human: A hospital patient has the following information:
        Average systolic rate: {avg_systolic} 
        Average diastolic rate: {avg_diastolic} 
        Average weight: {avg_weight} 
        Average BMI: {avg_bmi} 
        Average height: {avg_height} 
        Marital status: {marital_status} 
        Race: {race} 
        Breast cancer diagnosis status: {breast_cancer} 
        Breast cancer [female] diagnosis status: {breast_cancer_female} 
        Breast cancer [male] diagnosis status: {breast_cancer_male} 
        Type 2 diabetes diagnosis status: {type_2_diabetes} 
        Type 2 diabetes with ketoacidosis diagnosis status: {type_2_diabetes_with_ketoacidosis} 
        Type 2 diabetes with renal manifestations diagnosis status: {type_2_diabetes_with_renal_manifestations} 
        Type 2 diabetes with opthalmic manifestations diagnosis status: {type_2_diabetes_with_opthalmic_manifestations} 
        Type 2 diabetes with neurological manifestations diagnosis status: {type_2_diabetes_with_neurological_manifestations} 
        Chronic pancreatitis diagnosis status: {chronic_pancreatitis} 
        Given the above information, reply with 'Diagnosed' if the patient will be diagnosed with pancreatic cancer and 'Not diagnosed' if not. Only reply with 'Diagnosed' or 'Not diagnosed'. 
        Pancreatic cancer diagnosis status: \n bot: '{pancreatic_cancer}'"
        rows.append([prompt])
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
        csvwriter.writerows(rows)

# # csv_to_sql_hosp_drgcodes()
# # csv_to_sql_hosp_d_icd_diagnoses()
# patients_icd_codes()
# icd_to_phecodes()

# # csv_to_sql_hosp_admissions()
# # csv_to_sql_hosp_omr()

# admitted_patients()
# calculate_demographics()
# csv_to_sql_hosp_omr_summary()
# patients_phecodes_dischtimes_sql_hosp()

create_X_and_y_pancreatic_cancer_sql()
run_svm("X_and_y_pancreatic_cancer")

# create_csv_of_llm_prompts_pancreatic_cancer()