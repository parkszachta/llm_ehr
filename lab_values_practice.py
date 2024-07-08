with open('mimic-iv-2.2/hosp/labevents.csv', 'r') as file:
        total_lines = len(file.readlines())
with open('mimic-iv-2.2/hosp/labevents.csv', 'r') as file:
    current_line_num = 1
    while current_line_num <= 1000:
        current_line = file.readline().split(',')
        print(current_line)
        current_line_num += 1