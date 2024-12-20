from operator import length_hint

from bs4 import BeautifulSoup
import requests
import pandas as pd

# URL сайта теннисного магазина "SaleTennis.com"
# url = "https://saletennis.com/catalog/tennisnye-raketki/"
base_url = "https://saletennis.com"
main_url = "https://saletennis.com/catalog/tennisnye-raketki/"

try:
    # Отправляем GET запрос
    response = requests.get(main_url)
    #Проверим статус ответа (Вызовет исключение, если статус >= 400)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Находим все карточки товаров на странице
    products = soup.find_all('div', class_ = 'c-item c-item--expanded')

    # Создадим списки для хранения файла
    data = {
        "Название": [],
        "Цена": [],
        "Размеры": [],
        "Статус": []
    }

    # Все URL со страницы
    url_list = []
    # Извлекаем информацию для каждого продукта
    for product in products:
        # Название товара
        title_tag = product.find('a', class_ = 'c-item__title')
        title = title_tag.text.strip() if title_tag else 'Название не найдено'
        data['Название'].append(title)

        # price_tag = soup.find('meta', itemprop="price")
        # price_str = price_tag['content'].replace(',', '.') if price_tag else "Цена не найдена"
        # product_item["Цена"] = float(price_str) if price_str != "Цена не найдена" else "Цена не найдена"

        # Цена товара
        price_tag = product.find('p', class_='c-item__price')
        if price_tag:
            price_text = price_tag.text.strip()
            parts = price_text.split()
            # Берем последний элемент - это новая цена
            price = parts[0].rstrip('₽')
            price = price.split("₽")[-1].strip().split()
            data['Цена'].append(price)
        else:
            data['Цена'].append('Цена не найдена')

            # Размеры ручки ракетки
        size_tag = product.find('p', class_='c-item__sizes')
        size = size_tag.text.replace(" ", "").strip('Размеры') if size_tag else 'Размер не найден'
        data['Размеры'].append(size)

        # Статус товара
        status_tag = product.find('span', class_='c-item__label c-item__label--top')
        status = status_tag.text.strip() if status_tag else '-'
        data['Статус'].append(status)

        relative_url = product.find('a', class_='c-item__title')['href']
        absolute_url = base_url + relative_url
        url_list.append(absolute_url)

    # Преобразуем в DataFrame
    df = pd.DataFrame(data)

    # Сохраним df в json формат
    filename = "tennis_racket_data.json"

    df.to_json(filename, orient="records", force_ascii=False, indent = 4)
    print("Успешное подключение к сайту!")
    print(f"Данные сохранены в файл {filename}")


    product_details = []
    for url in url_list:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        product_item = {
            "Название": [],
            "Артикул": [],
            "Цена": [],
            "Размер головы (кв.см.)": [],
            "Вес без натяжки (гр.)": [],
            "Длина:": [],
            "Материал:": []
        }

        # Название ракетки
        name_tag = soup.find('h1', class_='card__title', itemprop = "name")
        name = name_tag.text.strip() if name_tag else 'Название не найдено'
        product_item["Название"].append(name)

        # Артикул
        art_tag = soup.find('p', class_='card__code')
        art = art_tag.text.strip().strip('Артикул').strip() if art_tag else 'Артикул не найден'
        product_item["Артикул"].append(art)

        #Цена
        price_tag = soup.find('meta', itemprop="price")
        price_str = price_tag['content'].replace(',', '.') if price_tag else "Цена не найдена"
        product_item["Цена"] = float(price_str) if price_str != "Цена не найдена" else "Цена не найдена"

        # Размер головы (кв.см.)
        caption_tag = soup.find('span', class_='features__caption', string='Размер головы (кв.см.):')
        if caption_tag:
            head_size_tag = caption_tag.find_next_sibling('span', class_='features__value')
            head_size = head_size_tag.text.strip() if head_size_tag else 'Размер головы не найден'
            head_size = int(head_size)
            product_item["Размер головы (кв.см.)"] = head_size
        else:
            product_item["Размер головы (кв.см.)"] = 'Размер не найден'

        # Вес без натяжки (гр.):
        weight_caption_tag = soup.find('span', class_='features__caption', string='Вес без натяжки (гр.):')
        if weight_caption_tag:
            weight_tag = weight_caption_tag.find_next_sibling('span', class_='features__value')
            weight = weight_tag.text.strip() if weight_tag else 'Вес не найден'
            weight = int(weight)
            product_item["Вес без натяжки (гр.)"] = weight
        else:
            product_item["Вес без натяжки (гр.)"] = 'Вес не найден'

        # Длина:
        length_caption_tag = soup.find('span', class_='features__caption', string='Длина:')
        if length_caption_tag:
            length_tag = length_caption_tag.find_next_sibling('span', class_='features__value')
            length = length_tag.text.strip().replace(',', '.') if length_tag else 'Длина не найденa'
            length = float(length)
            product_item["Длина:"] = length
        else:
            product_item["Длина:"] = 'Длина не найденa'

        # Материал
        material_caption_tag = soup.find('span', class_='features__caption', string='Материал:')
        if material_caption_tag:
            material_tag = material_caption_tag.find_next_sibling('span', class_='features__value')
            material = material_tag.text.strip() if material_tag else 'Материал не найден'
            product_item["Материал:"] = material
        else:
            product_item["Материал:"] = 'Материал не найден'

        product_details.append(product_item)
        # print(product_item)

    details = pd.DataFrame(product_details)

    # Сохраним df в json формат
    filename_1 = "tennis_racket_details_data.json"
    details.to_json(filename_1, orient="records", force_ascii=False, indent=4)
    print(f"Данные сохранены в файл {filename_1}")

except requests.exceptions.RequestException as e:
    print(f"Произошла ошибка при запросе: {e}")
