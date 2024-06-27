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
    # Few-shot prompting
    predicted_dischtime = llama3(
        f"Consider the following patients.\n\nPatient 1 had an admission to the hospital in YYYY-MM-DD HH:MM:SS format 
        of 2180-05-06 22:23:00, admission type of URGENT, location of admission of 
        TRANSFER FROM HOSPITAL, insurance status of Other, language of ENGLISH, 
        marital status of WIDOWED, race of WHITE, emergency room admission of 
        2180-05-06 19:17:00, and description of OTHER DISORDERS OF THE LIVER,
        \"DISORDERS OF LIVER EXCEPT MALIG,CIRR,ALC HEPA W CC\"HEPATIC COMA & OTHER MAJOR ACUTE 
        LIVER DISORDERS\"DISORDERS OF LIVER EXCEPT MALIG,CIRR,ALC HEPA W CC\"OTHER DISORDERS OF THE LIVER
        \"DISORDERS OF LIVER EXCEPT MALIG,CIRR,ALC HEPA W CC\"OTHER CIRCULATORY SYSTEM DIAGNOSES
        SYNCOPE & COLLAPSE. Their discharge time in YYYY-MM-DD HH:MM:SS format was 2180-05-07 17:15:00.\n\nPatient 2 had an admission to the hospital in YYYY-MM-DD HH:MM:SS format 
        of 2160-03-03 23:16:00, admission type of EU OBSERVATION, location of admission of 
        EMERGENCY ROOM, insurance status of Other, language of ENGLISH, 
        marital status of SINGLE, race of WHITE, emergency room admission of 
        2160-03-03 21:55:00, and description of . Their discharge time in YYYY-MM-DD HH:MM:SS format was 
        2160-03-04 06:26:00.\n\nPatient 3 had an admission to the hospital in YYYY-MM-DD HH:MM:SS format 
        of 2160-11-21 01:56:00, admission type of EW EMER., location of admission of 
        WALK-IN/SELF REFERRAL, insurance status of Medicare, language of ENGLISH, 
        marital status of MARRIED, race of WHITE, emergency room admission of 
        2160-11-20 20:36:00, and description of DEGENERATIVE NERVOUS SYSTEM DISORDERS EXC MULT SCLEROSIS
        DEGENERATIVE NERVOUS SYSTEM DISORDERS W/O MCC. Their discharge time in YYYY-MM-DD HH:MM:SS format was 
        2160-11-25 14:52:00.\n\nPatient 4 had an admission to the hospital in YYYY-MM-DD HH:MM:SS format 
        of {admittime}, admission type of {admission_type}, location of admission of
        {admission_location}, insurance status of {insurance}, language of {language}, 
        marital status of {marital_status}, race of {race}, emergency room admission of 
        {edregtime}, and description of {description}. Predict Patient 4's discharge time 
        from the hospital. The format of your answer should be in YYYY-MM-DD HH:MM:SS. 
        Do not provide any information except for that answer."
)
    # Few-shot and chain-of-thought prompting
    predicted_dischtime = llama3(
        f'''Consider the following patients.\n\nPatient 1 had an admission to the hospital in YYYY-MM-DD HH:MM:SS format 
        of 2180-05-06 22:23:00, admission type of URGENT, location of admission of 
        TRANSFER FROM HOSPITAL, insurance status of Other, language of ENGLISH, 
        marital status of WIDOWED, race of WHITE, emergency room admission of 
        2180-05-06 19:17:00, and description of OTHER DISORDERS OF THE LIVER,
        \"DISORDERS OF LIVER EXCEPT MALIG,CIRR,ALC HEPA W CC\"HEPATIC COMA & OTHER MAJOR ACUTE 
        LIVER DISORDERS\"DISORDERS OF LIVER EXCEPT MALIG,CIRR,ALC HEPA W CC\"OTHER DISORDERS OF THE LIVER
        \"DISORDERS OF LIVER EXCEPT MALIG,CIRR,ALC HEPA W CC\"OTHER CIRCULATORY SYSTEM DIAGNOSES
        SYNCOPE & COLLAPSE. Their discharge time in YYYY-MM-DD HH:MM:SS format was 2180-05-07 17:15:00.\n\nPatient 2 had an admission to the hospital in YYYY-MM-DD HH:MM:SS format 
        of 2160-03-03 23:16:00, admission type of EU OBSERVATION, location of admission of 
        EMERGENCY ROOM, insurance status of Other, language of ENGLISH, 
        marital status of SINGLE, race of WHITE, emergency room admission of 
        2160-03-03 21:55:00, and description of . Their discharge time in YYYY-MM-DD HH:MM:SS format was 
        2160-03-04 06:26:00.\n\nPatient 3 had an admission to the hospital in YYYY-MM-DD HH:MM:SS format 
        of 2160-11-21 01:56:00, admission type of EW EMER., location of admission of 
        WALK-IN/SELF REFERRAL, insurance status of Medicare, language of ENGLISH, 
        marital status of MARRIED, race of WHITE, emergency room admission of 
        2160-11-20 20:36:00, and description of DEGENERATIVE NERVOUS SYSTEM DISORDERS EXC MULT SCLEROSIS
        DEGENERATIVE NERVOUS SYSTEM DISORDERS W/O MCC. Their discharge time in YYYY-MM-DD HH:MM:SS format was 
        2160-11-25 14:52:00.\n\nPatient 4 had an admission to the hospital in YYYY-MM-DD HH:MM:SS format 
        of {admittime}, admission type of {admission_type}, location of admission of 
        {admission_location}, insurance status of {insurance}, language of {language}, 
        marital status of {marital_status}, race of {race}, emergency room admission of 
        {edregtime}, and description of {description}. Predict Patient 4's discharge time 
        from the hospital. The format of your answer should be in YYYY-MM-DD HH:MM:SS. 
        Do not provide any information except for that answer. Make sure to think step-by-step
        in detail!'''
    )