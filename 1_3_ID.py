def read_file():
    with open(r'C:\Users\ipavl\Учеба УРФУ\1 курс\1 семестр\ИД\86\third_task.txt', encoding="utf-8") as file:
        lines = file.readlines()
        table = []
        for line in lines:
            words = line.strip().split(" ")
            table.append(words)
        return table

def fill_na(table):
    for i in range(len(table)):
        for j in range(len(table[i])):
            if table[i][j] == 'N/A':
                table[i][j] = (table[i][j-1] + int(table[i][j+1])) / 2
            else:
                table[i][j] = int(table[i][j])

def apply_operation_6(table):
    result = []
    for row in table:
        count = 0
        sum = 0
        for num in row:
            if num % 5 == 0:
                count += 1
                sum  += num
        result.append(sum / count)
    return result

def write_to_file(column):
    with open("third_task_result.txt", "w", encoding="utf-8") as file:
        for num in column:
            file.write(f"{num}\n")

table = read_file()
fill_na(table)
result = apply_operation_6(table)
write_to_file(result)

