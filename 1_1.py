from os import write

# Читаем файл
def read_file():
    with open(r'../86/first_task.txt', encoding="utf-8") as file:
        return file.readlines()

# Разбиваем текст на слова и удаляем ненужные знаки припинания
def text_to_words(lines):
    words = []

    for line in lines:
        _line = (line
                .replace(",", "")
                .replace("?", "")
                .replace("!", "")
                .replace(".", "")
                .replace("-", " ")
                .lower().strip()
                )
        words += _line.split(" ")
    return words

# Считаем с какой частотой каждое слово встречается в тексте
def calc_freq(words):
    word_freq = {}
    for word in words:
        if len(word) == 0:
            continue
        if word in word_freq:
            word_freq[word] += 1
        else:
            word_freq[word] = 1

    return sorted(word_freq.items(), key = lambda x: x[1], reverse = True)

# Подсчитаем количество, а также долю всех согласных букв в тексте
def count_consonant(words):
    consonants = 0
    all_letters = 0
    vowel = 'aeiouyAEIOUY'
    for word in words:
        for letter in word:
            if letter.isalpha():
                all_letters += 1
                if letter not in vowel:
                    consonants += 1
    consonants_ratio = consonants / all_letters

    with open("first_task_result_1.txt", 'w', encoding = "utf-8") as f:
        f.write(f"Количество согласных букв: {consonants}\n")
        f.write(f"Доля согласных букв: {consonants_ratio:.2f}")
    return consonants, consonants_ratio

# Запись файла
def write_to_file(stat):
    with open("first_task_result.txt", "w", encoding = "utf-8") as file:
        for key, val in stat:
            file.write(f"{key}:{val}\n")

lines = read_file()
words = text_to_words(lines)
word_freq = calc_freq(words)
count_cons = count_consonant(words)
write_to_file(word_freq)

print(word_freq)
print(count_cons)
