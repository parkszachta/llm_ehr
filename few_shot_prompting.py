from llm_old import llama3

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