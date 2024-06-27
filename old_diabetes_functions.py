import sqlite3
from sklearn.model_selection import train_test_split
from sklearn import svm
from sklearn import metrics

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