def read_file():
    with open(r'C:\Users\ipavl\Учеба УРФУ\1 курс\1 семестр\ИД\86\second_task.txt', encoding="utf-8") as file:
        lines = file.readlines()
        table = []
        for line in lines:
            words = line.strip().split(" ")
            table.append(list(map(int, words)))
        return table

def first_operation(table):
    result = []
    for row in table:
        negative_sum = 0
        for num in row:
            if num < 0:
                negative_sum += num

        result.append(abs(negative_sum))
    return result

def find_average(result):
    return sum(result) / len(result)

def write_to_file(result, average_column):
    with open("second_task_result.txt", "w", encoding="utf-8") as file:
        for num in result:
            file.write(f"{num}\n")
        file.write(f"\n{average_column}\n")

table = read_file()
column = first_operation(table)
average_column = find_average(column)
write_to_file(column, average_column)


