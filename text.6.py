import json
import os
import requests
from bs4 import BeautifulSoup


str_json_file = ''
with open('data_wb.json', encoding='utf-8') as file:
    lines = file.readlines()
    for line in lines:
        str_json_file += line

data = json.loads(str_json_file)
# data = ['data']

#print(data)

soup = BeautifulSoup("""<table>
    <tr>
        <th>locale</th>
        <th>name</th>
        <th>sign</th>
        <th>code</th>
    </tr> 
    </table>""", "html.parser")

table = soup.contents[0]

for tick in data:
    tr = soup.new_tag("tr")

    for key, val in tick.items():
        td = soup.new_tag("td")
        td.string = str(val)
        tr.append(td)
    table.append(tr)

with open('r_text_6.html', encoding='utf-8', mode='w') as result:
    result.write(soup.prettify())
    result.write("\n")


