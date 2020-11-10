# This is a Python script to scrape business info from Yelp.

import requests
import csv
from datetime import datetime
import io

# set global variables
url = 'https://www.bbb.org/api/search'
now = datetime.now()
input_file = 'Keywords and Cities.csv'
# output_file = 'business_data_' + now.strftime("%m_%d_%Y_%H%M%S") + '.csv'
output_file = 'business_data_10_21_2020_124950.csv'
columns = ['Business', 'PhoneNumber', 'City', 'Keyword']
limit = 50
max_count = 300
# is_new = True
is_new = False

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"
}


# read input file and get cities and keywords
def read_file():
    locations = []
    terms = []
    try:
        with io.open(input_file, encoding='utf-8', errors='ignore') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0

            for row in csv_reader:
                try:
                    city = row[0]
                    keyword = row[1]
                    if line_count == 0:
                        line_count += 1
                        continue
                    if city:
                        if len(city.split('(')) > 1:
                            city = city.split('(')[0]
                        city = city.strip()
                        if city not in locations:
                            locations.append(city)
                            print(city)
                    if keyword:
                        terms.append(keyword)
                except:
                    pass
    except:
        print('Invalid Input File')
    return [locations, terms]


# run GraphQL query
def run_query(location, keyword, page_number, touched):
    param = {
        'find_country': 'USA',
        'find_loc': location,
        'find_text': keyword,
        'find_type': 'Category',
        'page': page_number,
        'touched': touched
    }

    cookies = {
        'iabbb_accredited_search': 'true',
        'iabbb_user_culture': 'en - us',
        'iabbb_user_postalcode': '21117',
        'iabbb_user_bbb': '0011'
    }

    request = requests.get(url, headers=headers, params=param, cookies=cookies)

    res = request.json()
    if request.status_code == 200:
        return res
    else:
        return 'error'


# save data to CSV
def save_data(rows, is_first):
    try:
        with open(output_file, mode='a', newline='') as data_set:
            writer = csv.DictWriter(data_set, columns)
            if is_first:
                writer.writeheader()
            writer.writerows(rows)
    except:
        pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("Started. Please wait a few minutes... \n")

    input_data = read_file()
    exit()
    if len(input_data[0]) > 0 and len(input_data[1]) > 0:
        is_first = is_new
        token_index = 0
        for location in input_data[0]:
            rows = []
            for term in input_data[1]:
                print('\nProcessing - City: {},\t Keyword: {}\t'.format(location, term), end='')
                page = 1
                total = 0
                count = 0
                while True:
                    # loop query
                    res = dict(run_query(location, term, page, 2))

                    # if res == 'error':
                    #     token_index += 1
                    #     if token_index < token_count:
                    #         print('\nNew token!' + str(token_index + 1))
                    #         continue
                    #     else:
                    #         print('\nDone. API token has been limited!')
                    #         exit()

                    if res == 'error':
                        print('\nQuery skipped: {}, {}, {}'.format(location, term, str(page)))
                        break

                    data = res.get('results')
                    total = int(res.get('totalResults'))

                    if data is None or len(data) == 0:
                        break

                    count += len(data)

                    print('...', end='')
                    for row in data:
                        if row is not None and row['accreditedCharity']:
                            row = dict(row)
                            b_phone = row.get('phone')
                            if b_phone is not None and len(b_phone) > 0:
                                b_phone = b_phone[0]
                            else:
                                continue

                            b_name = row.get('businessName')
                            if b_name is not None:
                                b_name = b_name.replace('<em>', '')
                                b_name = b_name.replace('</em>', '')
                            else:
                                continue

                            rows.append({
                                'Business': b_name,
                                'PhoneNumber': b_phone,
                                'City': location,
                                'Keyword': term
                            })

                    page += 1
                    if page > 15 or count >= total:
                        break

            save_data(rows, is_first)
            is_first = False

        print('\nFinished successfully!')