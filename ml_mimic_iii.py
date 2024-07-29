import numpy as np
import sqlite3
from datetime import datetime
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import SMOTE
from sklearn import svm as svm_callee
from sklearn import metrics
from sklearn import linear_model
from sklearn.ensemble import RandomForestClassifier

def calculate_demographics_helper(subject_ids_total, subject_ids_excluded):
    subject_ids_included = subject_ids_total - subject_ids_excluded
    gender_list = []
    age_list = []
    subject_ids_included_but_outlier_age = set()
    con = sqlite3.connect("patients_gender_and_age_mimic_iii.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM patients_gender_and_age_mimic_iii;")
    for row in cur.fetchall():
        print(row)
    with open('mimic-iii-clinical-database-1.4/PATIENTS.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iii-clinical-database-1.4/PATIENTS.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            subject_id = current_line[1]
            if current_line_num > 1 and subject_id in subject_ids_total and subject_id in subject_ids_included:
                cur = con.cursor()
                print(subject_id)
                subject_id_for_query = f""
                cur.execute(f"SELECT * FROM patients_gender_and_age_mimic_iii WHERE subject_id = \'\"{subject_id}\"\'")
                fetched_data = cur.fetchone()
                print(fetched_data)
                gender = fetched_data[1]
                age = fetched_data[2]
                # Only include patients with ages in between 5th and 95th percentiles
                if age > 0.000990569625066545 and age < 88.25872975131189:
                    gender_list.append(gender)
                    age_list.append(float(age))
                else:
                    subject_ids_included_but_outlier_age.add(subject_id)
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
    num_people_age = len(age_list)
    print(f"Number of people for age: {num_people_age}")
    print(f"Median age: {np.median(age_list)}")
    q25, q75 = np.percentile(a=age_list, q=[25, 75])
    iqr_age = q75 - q25
    print(f"Interquartile range age: {iqr_age}")
    con = sqlite3.connect("admitted_patients_mimic_iii.db")
    cur = con.cursor()
    subject_ids_included = subject_ids_included - subject_ids_included_but_outlier_age
    subject_ids_included = repr(subject_ids_included)
    subject_ids_included = "()" if subject_ids_included == "set()" else subject_ids_included.replace('{', '(').replace('}', ')')
    cur.execute(f"SELECT ethnicity FROM admitted_patients_mimic_iii WHERE subject_id IN {subject_ids_included};")
    ethnicity_list = []
    for row in cur.fetchall():
        ethnicity_list.append(row[0])
    ethnicity_counts = np.unique(ethnicity_list, return_counts=True)
    print(ethnicity_counts)
    num_people_ethnicity = np.sum(ethnicity_counts[1])
    names_of_ethnicity_categories = ethnicity_counts[0]
    counts_of_ethnicity_categories = ethnicity_counts[1]
    black_categories = ['"BLACK/AFRICAN AMERICAN"', '"BLACK/AFRICAN"', '"BLACK/CAPE VERDEAN"', '"BLACK/HAITIAN"']
    black_count = 0
    white_categories = ['"WHITE - BRAZILIAN"', '"WHITE - EASTERN EUROPEAN"', '"WHITE - OTHER EUROPEAN"', '"WHITE - RUSSIAN"', '"WHITE"']
    white_count = 0
    other_count = 0
    for i in range(len(names_of_ethnicity_categories)):
        name_of_ethnicity_category = names_of_ethnicity_categories[i]
        count_of_ethnicity_category = counts_of_ethnicity_categories[i]
        if name_of_ethnicity_category in black_categories:
            black_count += count_of_ethnicity_category
        elif name_of_ethnicity_category in white_categories:
            white_count += count_of_ethnicity_category
        else:
            other_count += count_of_ethnicity_category
    print(f"Number of people for ethnicity: {num_people_ethnicity}")
    print(f"Number of BLACK: {black_count}")
    print(f"Proportion of BLACK: {black_count / num_people_ethnicity}")
    print(f"Number of WHITE: {white_count}")
    print(f"Proportion of WHITE: {white_count / num_people_ethnicity}")
    print(f"Number of Other: {other_count}")
    print(f"Proportion of Other: {other_count / num_people_ethnicity}")

def calculate_demographics():
    con = sqlite3.connect("admitted_patients_mimic_iii.db")
    cur = con.cursor()
    cur.execute("SELECT subject_id FROM admitted_patients_mimic_iii;")
    admitted_patients = cur.fetchall()
    admitted_patients_temp = set()
    for item in admitted_patients:
        admitted_patients_temp.add(item[0])
    admitted_patients = admitted_patients_temp
    con2 = sqlite3.connect("patients_phecodes_dischtimes_mimic_iii.db")
    cur2 = con2.cursor()
    cur2.execute("SELECT subject_id FROM patients_phecodes_dischtimes_mimic_iii;")
    subject_ids = cur2.fetchall()
    print(subject_ids)
    print("subject_ids before ^^^")
    subject_ids_temp = set()
    for item in subject_ids:
        subject_ids_temp.add(item[0])
    subject_ids = subject_ids_temp.intersection(admitted_patients)
    cur2.execute("SELECT subject_id FROM patients_phecodes_dischtimes_mimic_iii WHERE `157` = 0;")
    subject_ids_not_diagnosed_with_157 = cur2.fetchall()
    subject_ids_not_diagnosed_with_157_temp = set()
    for item in subject_ids_not_diagnosed_with_157:
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

def csv_to_sql_hosp_admissions():
    con = sqlite3.connect("admissions_mimic_iii.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS admissions_mimic_iii;")
    cur.execute("CREATE TABLE admissions_mimic_iii (subject_id, hadm_id, admittime, dischtime, deathtime, marital_status, ethnicity);")
    with open('mimic-iii-clinical-database-1.4/ADMISSIONS.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iii-clinical-database-1.4/ADMISSIONS.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                print(current_line_num)
                subject_id = current_line[1]
                hadm_id = current_line[2]
                admittime = current_line[3]
                dischtime = current_line[4]
                deathtime = current_line[5]
                marital_status = current_line[12]
                ethnicity = current_line[13]
                print(f"{subject_id} {admittime} {dischtime} {deathtime} {marital_status} {ethnicity}")
                cur.execute("INSERT INTO admissions_mimic_iii (subject_id, hadm_id, admittime, dischtime, deathtime, marital_status, ethnicity) VALUES (?, ?, ?, ?, ?, ?, ?);",
                            (subject_id, hadm_id, admittime, dischtime, deathtime, marital_status, ethnicity))
                con.commit()
            current_line_num += 1
    cur = con.cursor()
    cur.execute("SELECT * FROM admissions_mimic_iii;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def csv_to_sql_hosp_drgcodes():
    con = sqlite3.connect("drgcodes_mimic_iii.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS drgcodes_mimic_iii;")
    cur.execute("CREATE TABLE drgcodes_mimic_iii (subject_id, hadm_id, drg_type, drg_code, description, drg_severity, drg_mortality);")
    with open('mimic-iii-clinical-database-1.4/DRGCODES.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iii-clinical-database-1.4/DRGCODES.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                print(current_line_num)
                print(current_line)
                subject_id = current_line[1]
                hadm_id = current_line[2]
                drg_type = current_line[3]
                drg_code = current_line[4]
                description = ""
                i = 5
                while i < len(current_line) - 3:
                    description += current_line[i]
                    description += ","
                    i += 1
                description += current_line[len(current_line) - 3]
                drg_severity = current_line[len(current_line) - 2]
                drg_mortality = current_line[len(current_line) - 1]
                drg_mortality = drg_mortality.split('\n')[0]
                cur.execute("INSERT INTO drgcodes_mimic_iii (subject_id, hadm_id, drg_type, drg_code, description, drg_severity, drg_mortality) VALUES (?, ?, ?, ?, ?, ?, ?);",
                            (subject_id, hadm_id, drg_type, drg_code, description, drg_severity, drg_mortality))
                con.commit()
            current_line_num += 1
    cur = con.cursor()
    cur.execute("SELECT * FROM drgcodes_mimic_iii;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def patients_gender_and_age():
    con = sqlite3.connect("patients_gender_and_age_mimic_iii.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS patients_gender_and_age_mimic_iii;")
    cur.execute("CREATE TABLE patients_gender_and_age_mimic_iii (subject_id, gender, age);")
    con2 = sqlite3.connect("admitted_patients_mimic_iii.db")
    cur2 = con2.cursor()
    batch_data = []
    with open('mimic-iii-clinical-database-1.4/PATIENTS.csv', 'r') as file:
        total_lines = len(file.readlines())
    with open('mimic-iii-clinical-database-1.4/PATIENTS.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            print(current_line_num)
            current_line = file.readline().split(',')
            if current_line_num > 1:
                _, subject_id, gender, dob, _, _, _, _ = current_line
                print(subject_id)
                print(dob)
                dob = datetime.strptime(dob, '%Y-%m-%d %H:%M:%S')
                cur2.execute(f"SELECT first_admittime FROM admitted_patients_mimic_iii WHERE subject_id = '{subject_id}';")
                fetched_data = cur2.fetchone()
                print(fetched_data)
                if fetched_data is not None:
                    print(fetched_data[0])
                    first_admittime = datetime.strptime(fetched_data[0], '%Y-%m-%d %H:%M:%S')
                    age = first_admittime - dob
                    age = age.total_seconds() / (365.25 * 24 * 60 * 60)
                    batch_data.append([f'\"{subject_id}\"', gender, age])
                    print([f'\"{subject_id}\"', gender, age])
                    print(f"NOT YET: INSERT INTO patients_gender_and_age_mimic_iii (subject_id, gender, age) VALUES ({subject_id}, {gender}, {age})")
            current_line_num += 1
    cur.executemany("INSERT INTO patients_gender_and_age_mimic_iii (subject_id, gender, age) VALUES (?, ?, ?);",
                    batch_data)
    cur = con.cursor()
    cur.execute("SELECT * FROM patients_gender_and_age_mimic_iii;")
    print(cur.fetchall())
    con.commit()
    con.close()

def admitted_patients():
    con = sqlite3.connect("admissions_mimic_iii.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM admissions_mimic_iii;")
    con2 = sqlite3.connect("admitted_patients_mimic_iii.db")
    cur2 = con2.cursor()
    cur2.execute("DROP TABLE IF EXISTS admitted_patients_mimic_iii;")
    cur2.execute("CREATE TABLE admitted_patients_mimic_iii (subject_id, first_admittime, marital_status, ethnicity);")
    subject_ids_already_inputted = set()
    for row in cur.fetchall():
        subject_id = row[0]
        if subject_id not in subject_ids_already_inputted:
            subject_ids_already_inputted.add(subject_id)
            admittime = row[2]
            marital_status = row[5]
            ethnicity = row[6]
            cur2.execute("INSERT INTO admitted_patients_mimic_iii (subject_id, first_admittime, marital_status, ethnicity) VALUES (?, ?, ?, ?);",
                (subject_id, admittime, marital_status, ethnicity))
            print(f"INSERT INTO admitted_patients_mimic_iii (subject_id, first_admittime, marital_status, ethnicity) VALUES ({subject_id}, {admittime}, {marital_status}, {ethnicity});")
    con2.commit()
    con.commit()
    cur2.execute("SELECT * FROM admitted_patients_mimic_iii;")
    for row in cur2.fetchall():
        print(row)
    con2.commit()
    con.close()
    con2.close()

def patients_icd_codes():
    con = sqlite3.connect(f"patients_icd9_codes_mimic_iii.db")
    cur = con.cursor()
    cur.execute(f"DROP TABLE IF EXISTS patients_icd9_codes_mimic_iii;")
    cur.execute(f"CREATE TABLE patients_icd9_codes_mimic_iii (subject_id, hadm_id, icd9_code);")
    with open('mimic-iii-clinical-database-1.4/DIAGNOSES_ICD.csv', 'r') as file:
        total_lines = len(file.readlines())
    current_subject_id = ""
    current_hadm_id = ""
    current_icd9_codes = []
    with open('mimic-iii-clinical-database-1.4/DIAGNOSES_ICD.csv', 'r') as file:
        current_line_num = 1
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            if current_line_num > 1:
                print(current_line)
                subject_id = current_line[1]
                hadm_id = current_line[2]
                icd9_code = current_line[4].split('\n')[0]
                if hadm_id != current_hadm_id:
                    if current_hadm_id != "":
                        for current_icd9_code in current_icd9_codes:
                            cur.execute("INSERT INTO patients_icd9_codes_mimic_iii (subject_id, hadm_id, icd9_code) VALUES (?, ?, ?);", 
                                (current_subject_id, current_hadm_id, current_icd9_code))
                    current_subject_id = subject_id
                    current_hadm_id = hadm_id
                    current_icd9_codes = []
                current_icd9_code = icd9_code
                if current_icd9_code not in current_icd9_codes:
                    current_icd9_codes.append(icd9_code)
            current_line_num += 1
    for current_icd9_code in current_icd9_codes:
        cur.execute("INSERT INTO patients_icd9_codes_mimic_iii (subject_id, hadm_id, icd9_code) VALUES (?, ?, ?);", 
            (current_subject_id, current_hadm_id, current_icd9_code))
    con.commit()
    cur.execute("SELECT * FROM patients_icd9_codes_mimic_iii;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def icd_to_phecodes():
    con = sqlite3.connect('icd_to_phecodes_mimic_iii.db')
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS icd_to_phecodes_mimic_iii")
    cur.execute(f"CREATE TABLE icd_to_phecodes_mimic_iii (ICD, flag, phecode);")
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
                cur.execute("INSERT INTO icd_to_phecodes_mimic_iii (ICD, flag, phecode) VALUES (?, ?, ?);", 
                            (ICD, flag, phecode))
            current_line_num += 1
    cur.execute("SELECT * FROM icd_to_phecodes_mimic_iii;")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def icd_to_phecodes_data_structures():
    con = sqlite3.connect('icd_to_phecodes_mimic_iii.db')
    cur = con.cursor()
    cur.execute("SELECT * FROM icd_to_phecodes_mimic_iii;")
    icd9_to_phecodes = {}
    for row in cur.fetchall():
        ICD = row[0]
        flag = row[1]
        phecode = row[2]
        if flag == 9:
            if ICD not in icd9_to_phecodes:
                icd9_to_phecodes[ICD] = set()
            icd9_to_phecodes[ICD].add(phecode)
        print(f"Phecode: {phecode}")
    return icd9_to_phecodes

def hadm_id_to_dischtimes_data_structure():
    con = sqlite3.connect('admissions_mimic_iii.db')
    cur = con.cursor()
    cur.execute("SELECT hadm_id, dischtime FROM admissions_mimic_iii;")
    hadm_id_to_dischtimes = {}
    for row in cur.fetchall():
        hadm_id = row[0]
        dischtime = row[1]
        hadm_id_to_dischtimes[hadm_id] = dischtime
        print(hadm_id)
        print(dischtime)
    print("end of hadm_id_to_dischtimes_data_structure")
    return hadm_id_to_dischtimes

def patients_phecodes_dischtimes_sql_hosp(phecodes_string_only=False, phecode_to_be_predicted="-1"):
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
    question_string = ""
    for phecode in phecodes:
        if not phecodes_string_only or phecode_to_be_predicted != phecode:
            phecodes_string += f"`{phecode}`, "
            default_string += "0, "
            question_string += "?, "
    phecodes_string = phecodes_string.removesuffix(", ")
    question_string = question_string.removesuffix(", ")
    if phecodes_string_only:
        return phecodes_string, question_string
    default_string = default_string.removesuffix(", ")
    print(phecodes_string)
    print(default_string)
    con = sqlite3.connect("patients_phecodes_dischtimes_mimic_iii.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS patients_phecodes_dischtimes_mimic_iii;")
    print(f"CREATE TABLE patients_phecodes_dischtimes_mimic_iii (subject_id, {phecodes_string});")
    cur.execute(f"CREATE TABLE patients_phecodes_dischtimes_mimic_iii (subject_id, {phecodes_string});")
    # Fill in the rows of patients_phecodes such that each row is a patient
    con2 = sqlite3.connect("admitted_patients_mimic_iii.db")
    cur2 = con2.cursor()
    cur2.execute("SELECT subject_id FROM admitted_patients_mimic_iii;")
    for row in cur2.fetchall():
        current_subject_id = row[0]
        print(f"Current_subject_id: {current_subject_id}")
        cur.execute(f"INSERT INTO patients_phecodes_dischtimes_mimic_iii VALUES (?, {default_string});", 
                    (current_subject_id,))
    # Map each subject_id to their phecodes
    icd9_to_phecodes = icd_to_phecodes_data_structures()
    hadm_id_to_dischtimes = hadm_id_to_dischtimes_data_structure()
    cur = con.cursor()
    con3 = sqlite3.connect("patients_icd9_codes_mimic_iii.db")
    cur3 = con3.cursor()
    cur3.execute("SELECT * FROM patients_icd9_codes_mimic_iii;")
    cur3 = con3.cursor()
    cur3.execute("SELECT * FROM patients_icd9_codes_mimic_iii;")
    subject_id_to_phecodes_dischtimes = {}
    for row in cur3.fetchall():
        print(f"cur3 row: {row}")
        current_subject_id = row[0]
        hadm_id = row[1]
        if row[2] != '':
            icd9_code = row[2].split('\"')[1]
            print(icd9_code)
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
            cur.execute(f"UPDATE patients_phecodes_dischtimes_mimic_iii SET {current_update_phrase} WHERE subject_id = '{current_subject_id}';")
            con.commit()
            print(f"UPDATE patients_phecodes_dischtimes_mimic_iii SET {current_update_phrase} WHERE subject_id = '{current_subject_id}';")
        print(f"current_subject_id: {current_subject_id}")
    con.commit()
    con2.commit()
    con3.commit()
    con2.close()
    con3.close()
    cur = con.cursor()
    cur.execute("SELECT * FROM patients_phecodes_dischtimes_mimic_iii")
    for row in cur.fetchall():
        print(row)
    con.commit()
    con.close()

def create_X_and_y(phecode_to_be_predicted):
    print(f"Starting create_X_and_y({phecode_to_be_predicted})")
    phecode_to_be_predicted_without_decimal = ""
    for item in phecode_to_be_predicted.split("."):
        phecode_to_be_predicted_without_decimal += item
    print(phecode_to_be_predicted_without_decimal)
    print(f"X_and_y_{phecode_to_be_predicted_without_decimal}_mimic_iii.db")
    con = sqlite3.connect(f"X_and_y_{phecode_to_be_predicted_without_decimal}_mimic_iii.db")
    cur = con.cursor()
    cur.execute(f"DROP TABLE IF EXISTS X_and_y_{phecode_to_be_predicted_without_decimal}_mimic_iii;")
    phecodes_string, question_string = patients_phecodes_dischtimes_sql_hosp(phecodes_string_only=True, phecode_to_be_predicted=phecode_to_be_predicted)
    # everything after `{phecode_to_be_predicted}` is X
    # `{phecode_to_be_predicted}` is y
    database_columns = f"`{phecode_to_be_predicted}`, marital_status, black, white, male, age, {phecodes_string}"
    question_string = f"?, ?, ?, ?, ?, ?, {question_string}"
    cur.execute(f"CREATE TABLE X_and_y_{phecode_to_be_predicted_without_decimal}_mimic_iii ({database_columns});")
    black_categories = ['"BLACK/AFRICAN AMERICAN"', '"BLACK/AFRICAN"', '"BLACK/CAPE VERDEAN"', '"BLACK/HAITIAN"']
    white_categories = ['"WHITE - BRAZILIAN"', '"WHITE - EASTERN EUROPEAN"', '"WHITE - OTHER EUROPEAN"', '"WHITE - RUSSIAN"', '"WHITE"']
    con2 = sqlite3.connect("admitted_patients_mimic_iii.db")
    cur2 = con2.cursor()
    cur2.execute("SELECT * FROM admitted_patients_mimic_iii;")
    con4 = sqlite3.connect("patients_phecodes_dischtimes_mimic_iii.db")
    cur4 = con4.cursor()
    con5 = sqlite3.connect("patients_gender_and_age_mimic_iii.db")
    cur5 = con5.cursor()
    batch_data = []
    for row in cur2.fetchall():
        subject_id = row[0]
        marital_status = 1 if row[2] == '"MARRIED"' else 0
        ethnicity = row[3]
        black = 1 if ethnicity in black_categories else 0
        white = 1 if ethnicity in white_categories else 0
        cur5.execute(f"SELECT gender, age FROM patients_gender_and_age_mimic_iii WHERE subject_id = \'\"{subject_id}\"\'")
        gender_and_age_row = cur5.fetchone()
        print(subject_id)
        print(gender_and_age_row)
        if gender_and_age_row is not None:
            male = 1 if gender_and_age_row[0] == "M" else 0
            age = float(gender_and_age_row[1])
            if age > 0.000990569625066545 and age < 88.25872975131189:
                cur4.execute(f"SELECT `{phecode_to_be_predicted}`, {phecodes_string} FROM patients_phecodes_dischtimes_mimic_iii WHERE subject_id = '{subject_id}';")
                diagnosed_times = cur4.fetchone()
                phecode_to_be_predicted_diagnosed_time = diagnosed_times[0]
                phecode_to_be_predicted_diagnosed = 1 if phecode_to_be_predicted_diagnosed_time != 0 else 0
                predictor_diagnosed_values = []
                for i in range(1, len(diagnosed_times), 1):
                    predictor_diagnosed_value = 1 if diagnosed_times[i] != 0 and (phecode_to_be_predicted_diagnosed == 0 or datetime.strptime(diagnosed_times[i], '%Y-%m-%d %H:%M:%S') < datetime.strptime(phecode_to_be_predicted_diagnosed_time, '%Y-%m-%d %H:%M:%S')) else 0
                    predictor_diagnosed_values.append(predictor_diagnosed_value)
                data_to_be_appended = [phecode_to_be_predicted_diagnosed, marital_status, black, white, male, age] + predictor_diagnosed_values
                batch_data.append(data_to_be_appended)
                insertion_string = ""
                for i in range(0, len(data_to_be_appended) - 1, 1):
                    insertion_string += str(data_to_be_appended[i])
                    insertion_string += ", "
                insertion_string += str(data_to_be_appended[len(data_to_be_appended) - 1])
                print(f"NOT YET: INSERT INTO X_and_y_{phecode_to_be_predicted_without_decimal}_mimic_iii ({database_columns}) VALUES ({insertion_string});")
                if len(batch_data) > 500:
                    cur.executemany(f"INSERT INTO X_and_y_{phecode_to_be_predicted_without_decimal}_mimic_iii ({database_columns}) VALUES ({question_string});",
                    batch_data)
                    batch_data = []
    cur.executemany(f"INSERT INTO X_and_y_{phecode_to_be_predicted_without_decimal}_mimic_iii ({database_columns}) VALUES ({question_string});",
                    batch_data)
    con.commit()
    con2.commit()
    con4.commit()
    con.close()
    con2.close()
    con4.close()
    print(f"Ending create_X_and_y({phecode_to_be_predicted})")

def logistic_regression(phecode_to_be_predicted_without_decimal):
    X = []
    y = []
    con = sqlite3.connect(f"X_and_y_{phecode_to_be_predicted_without_decimal}_mimic_iii.db")
    cur = con.cursor()
    cur.execute(f"SELECT * FROM X_and_y_{phecode_to_be_predicted_without_decimal}_mimic_iii;")
    for row in cur.fetchall():
        X.append(row[1:])
        y.append(row[0])
    con.commit()
    con.close()
    print(f"X length: {len(X)}")
    print(f"First few X: {X[0:10]}")
    print(f"y length: {len(y)}")
    print(f"First few y: {y[0:10]}")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1)
    print(f"First few X_train: {X_train[0:10]}")
    print(f"First few X_test: {X_test[0:10]}")
    print(f"First few y_train: {y_train[0:10]}")
    print(f"First few y_test: {y_test[0:10]}")
    print(f"X_train length: {len(X_train)}")
    print(f"y_train length: {len(y_train)}")
    print(f"y_train amount of 0: {y_train.count(0)}")
    print(f"y_train amount of 1: {y_train.count(1)}")
    # sm = SMOTE(random_state = 1)
    # X_train_smote, y_train_smote = sm.fit_resample(X_train, y_train)
    print(f"X_train length: {len(X_train)}")
    print(f"y_train length: {len(y_train)}")
    print(f"y_train amount of 0: {y_train.count(0)}")
    print(f"y_train amount of 1: {y_train.count(1)}")
    logreg = linear_model.LogisticRegression(class_weight='balanced', max_iter=10000)
    logreg.fit(X_train, y_train)
    y_probs = logreg.predict_proba(X_test)[:, 1]
    thresholds = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    confusion_matrices = []
    accuracies = []
    precisions = []
    recalls = []
    f1_scores = []
    AUCs = []
    for threshold in thresholds:
        y_pred = (y_probs >= threshold).astype(int)
        confusion_matrices.append(metrics.confusion_matrix(y_test, y_pred, labels=[0, 1]))
        accuracies.append(metrics.accuracy_score(y_test, y_pred))
        precisions.append(metrics.precision_score(y_test, y_pred))
        recalls.append(metrics.recall_score(y_test, y_pred))
        f1_scores.append(metrics.f1_score(y_test, y_pred))
        fpr, tpr, _ = metrics.roc_curve(y_test, y_pred)
        AUCs.append(metrics.auc(fpr, tpr))
    print(f"Confusion matrices: {confusion_matrices}")
    print(f"Accuracies: {accuracies}")
    print(f"Precisions: {precisions}")
    print(f"Recalls: {recalls}")
    print(f"F1-Scores: {f1_scores}")
    print(f"AUC: {AUCs}")

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
    clf = svm_callee.LinearSVC(class_weight='balanced')
    # clf = svm_callee.SVC(class_weight='balanced')
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

def random_forest(X_and_y_database_name):
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
    rf = RandomForestClassifier(class_weight='balanced')
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    cnf_matrix = metrics.confusion_matrix(y_test, y_pred, labels=[0, 1])
    print(cnf_matrix)
    print(f"Predicted labels length: {len(y_pred)}")
    print(f"First few predicted labels: {y_pred[0:20]}")
    print(f"Accuracy: {metrics.accuracy_score(y_test, y_pred)}")
    print(f"Precision: {metrics.precision_score(y_test, y_pred)}")
    print(f"Recall: {metrics.recall_score(y_test, y_pred)}")
    print(f"F1-Score: {metrics.f1_score(y_test, y_pred)}")

def create_medical_notes_file():
    subject_id_to_notes = {}
    con = sqlite3.connect("admitted_patients_mimic_iii.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM admitted_patients_mimic_iii;")
    for row in cur.fetchall():
        subject_id = row[0]
        subject_id_to_notes[subject_id] = ""
    with open('mimic-iii-clinical-database-1.4/NOTEEVENTS.csv', 'r') as file:
        total_lines = len(file.readlines())
        for _ in range(100):
            print(file.readline().split(','))
    with open('mimic-iii-clinical-database-1.4/NOTEEVENTS.csv', 'r') as file:
        current_line = file.readline().split(',')
        current_line_num = 2
        while current_line_num <= total_lines:
            current_line = file.readline().split(',')
            print(current_line)
            if len(current_line) > 1:
                subject_id = current_line[1]
                text = current_line[10] if len(current_line) > 10 else ""
                i = 11
                while i < len(current_line):
                    text += f",{current_line[i]}"
                    i += 1
                text = text.split('\n')[0]
                if subject_id in subject_id_to_notes.keys():
                    subject_id_to_notes[subject_id] += text
            print(current_line_num)
            current_line_num += 1
    with open('doctor_notes.txt', 'w') as txtfile:
        for current_text in subject_id_to_notes.values():
            txtfile.write(current_text)

# csv_to_sql_hosp_drgcodes()
# patients_icd_codes()
# icd_to_phecodes()
# csv_to_sql_hosp_admissions()
# admitted_patients()
# patients_gender_and_age()
# patients_phecodes_dischtimes_sql_hosp()
# calculate_demographics()

# create_X_and_y("157")
# create_X_and_y("250.2")

logistic_regression("157")
# logistic_regression("2502")

# svm("X_and_y_pancreatic_cancer_mimic_iii")
# random_forest("X_and_y_pancreatic_cancer_mimic_iii")

# create_medical_notes_file()
