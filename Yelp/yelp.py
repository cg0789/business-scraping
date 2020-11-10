# This is a Python script to scrape business info from Yelp.

import requests
import csv
from datetime import datetime
import mysql.connector
import io

# set global variables
token = [
    '4_uM2OY99tUiDbbQ8UoGGgBwW8hZicDGnWODA9ocYhjf8bG3aeB3eEa1h2q6ydv2td1400SJD8KBLg2IRCnLQtjwHxI8S26rWW2sYF22tGc35vc7Et834uE9otmqX3Yx',
    'eWTCrLjVnawpfBGvkgXacngMlMFEMUD5r8cwCzcPOnwWnyoEsCNcdXxZpBQmrRnC-JG_qPLNyQkP4_bWGKmuamWcmcCB5PXintC0XZYleygZiZH5BpSYMgvEELGAX3Yx',
    'A-JoROp4hq03yRLFx-ULBacAAbgCeEkdmv6fwG0ZPTnuJ1Tggypv-hZNbC9qUCHO0A-UrHlpP7eDU_s_VA6dhp4VDzEVP7EYkxaytGlLfPYYFuWRFo8-HT43wCiEX3Yx',
    '3h7cO7D_QTO1q3AcTgBy0XJCQbtghX5wdUZ8dbd8uPxQXWJxqDRl3-ReAy7OlSvajVi4FT_Y4RwJxaZ_fJs2bZSLDFtfU_kXUN4JSdQRaJq3fAwZYfrHuTQujNmBX3Yx',
    'EsKapATn6Dzek8-htQjvb8sAEQhYnzwpcfkqAe9N1flEF9ZE59utya7iIk9v2IWVOOIP52_nM-oC6jCbh169Nf6Kps41aOd7nkKozlP_FlMY2PKC7UTkTuIJWiuEX3Yx',
    'jztcbc7EhMChowjjzv5YzUNjj_Bqzbp7FZ6jKNlZtJkRZexNgiy4rvBL3XBf3FgglRQyzG93dT5Fbi9BehbSbjPjvDKKuhv1My16C73nhUX1HXkPyumELD0zXhMOX3Yx',
    'miu4_Lyo2_mAlSysuHde_UCfPwJOlQGLkSBuqCpj-lpyFbiWEqCN9HYmwUaFUaRHzqpppX-Wax1PFDtWMAwQos-QMrmopo_LXnDJRNIiigh8NhxWzfskFogfxtaEX3Yx',
    '2OGxedXP3btPjY_IBvhXXLiVzU0zxWMFu6k1-kfaMXLVWTDLq0jEU0BAbfOZuYwtlQ4ekB9iMac4iM25-rXw-U-pt71mpKA0I-WwsfC6rYydeg5Hr5ApztHmX9-FX3Yx',
    'F07nB95MXHtF4c1hEqWCBa-b0je3zJXXcpdRdQXROKj3ugkWj_IcYySG9AUaSW-Aadkv9FRXgTR38yMaxBN3YAgRCmqJsVUT0Cn6gFG9MtVIy4SxPgBCT-Pdzd-FX3Yx'
    ]

token_count = 1
url = 'https://api.yelp.com/v3/graphql'
now = datetime.now()
input_file = 'Keywords and Cities.csv'
output_file = 'business_data_' + now.strftime("%m_%d_%Y_%H%M%S") + '.csv'
# output_file = 'business_data_10_13_2020_031644.csv'
columns = ['Business', 'PhoneNumber', 'City', 'Keyword']
limit = 2
max_count = 2
# is_new = False
is_new = True
start_off = 0
token_index = 0
start_city = 'Sheboygan'
curr_date = '2020-11-10'
source = "Yelp"


# read input file and get cities and keywords
def read_file():
    locations = []
    terms = []
    is_new_city = False
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
                        if city == start_city:
                            is_new_city = True
                        if is_new_city:
                            if len(city.split('(')) > 1:
                                city = city.split('(')[0]
                            city = city.strip()
                            if city not in locations:
                                locations.append(city)
                                # print(city)
                    if keyword:
                        terms.append(keyword)
                except:
                    pass
    except:
        print('Invalid Input File')
    return [locations, terms]


# run GraphQL query
def run_query(query):
    header = {'Authorization': 'Bearer ' + token[token_index]}
    request = requests.post(url, json={'query': query}, headers=header)
    res = request.json()
    if request.status_code == 200:
        return res
    else:
        try:
            if res['errors'][0]['extensions']['code'] == 'DAILY_POINTS_LIMIT_REACHED':
                return 'limited'
            elif res['error']['code'] == 'TOKEN_INVALID':
                return 'limited'
            else:
                return 'error'
        except:
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


#
# def storeDataToDB(data):
#     try:
#         connection = mysql.connector.connect(host='localhost',
#                                              database='scraping',
#                                              user='root',
#                                              password='')
#
#         insert_query = "INSERT IGNORE INTO `business` (`Business`, `PhoneNumber`, `City`, `Keyword`) VALUES \n  "
#         rows = []
#
#         for i in range(len(data)):
#             row = data[i]
#             insert_query += "('{}', '{}', '{}', '{}')".format(
#                 row['name'].replace("'", "\'"),
#                 row['display_phone'],
#                 str(row[2]), str(mid))
#             if not i == N - 1:
#                 sql += ",\n"
#             else:
#                 sql += ";\n"
#
#         for row in data:
#             try:
#
#                 rows.append({
#                     'Business': row['name'].replace("'", "\'"),
#                     'PhoneNumber': row['display_phone'].replace("'", "\'"),
#                     'City': location,
#                     'Keyword': term
#                 })
#                 firstname = user['firstname']
#                 lastname = user['lastname']
#                 email = user['email']
#                 lastlogin = int(user['lastLogin'])
#                 date = datetime.datetime.fromtimestamp(lastlogin / 1e3)
#                 str_date = '{}-{}-{} {}:{}'.format(date.year, date.month, date.day, date.hour, date.minute)
#                 users.append((firstname + ' ' + lastname, email, str_date))
#             except:
#                 pass
#     cursor = connection.cursor()
#     cursor.executemany(mySql_insert_query, users)
#     connection.commit()
#
#
#     except mysql.connector.Error as error:
#         print("Failed to insert record into MySQL table {}".format(error))
#
#     finally:
#         if (connection.is_connected()):
#             cursor.close()
#             connection.close()
#
#             print("Done")


def get_query_by_location(keywords, location):
    query0 = """
        {
            search(term: "%s",
                location: "%s",
                limit: %s,
                offset: %s) {
                total
                business {
                    name
                    display_phone
                    is_claimed
                }
            }
        }
    """
    rows = []
    i = 0
    global token_index
    for term in keywords:
        print('\nProcessing - City: {},\t Keyword: {}\t'.format(location, term), end='')
        offset = start_off
        total = 0
        while True:
            # loop query
            query = query0 % (term, location, limit, offset)
            res = run_query(query)

            if res == 'limited':
                token_index += 1
                if token_index < token_count:
                    print('\nNew token!' + str(token_index + 1))
                    continue
                else:
                    print('\nDone. API token has been limited!')
                    exit()

            elif res == 'error':
                print('\nQuery skipped: {}, {}, {}'.format(location, term, str(offset)))
                break
            res = dict(res)
            data = res.get('data')
            if data is None:
                break

            search_info = data.get('search')
            if search_info is None:
                break

            business_info = search_info.get('business')
            if business_info is None or len(business_info) == 0:
                break

            print('...', end='')
            total = search_info.get('total')
            for row in business_info:
                if row is not None and row['is_claimed'] and row['display_phone'] and row['name']:
                    if "estate" in row['name'].lower() or "apartment" in row['name'].lower():
                        continue
                    # if i > 0:
                    #     insert_qry += ",\n"

                    rows.append((row['name'].replace("'", "\'"), row['display_phone'], location, term, curr_date, source))

                    # insert_qry += "('{}', '{}', '{}', '{}')".format(row['name'].replace("'", "\'"),
                    #                                                 row['display_phone'],
                    #                                                 location,
                    #                                                 term)
                    # rows.append({
                    #     'Business': row['name'],
                    #     'PhoneNumber': row['display_phone'],
                    #     'City': location,
                    #     'Keyword': term
                    # })

            offset += limit
            if offset >= start_off + max_count or total < offset:
                break
            # break
        # break
    # insert_qry += ";"
    return rows


def main():
    # Check mysql connection
    try:
        connection = mysql.connector.connect(host='localhost', database='scraping', user='root', password='')
        cursor = connection.cursor()

        input_data = read_file()

        if len(input_data[0]) > 0 and len(input_data[1]) > 0:
            # is_first = is_new
            insert_qry = "INSERT IGNORE INTO `google` (`Business`, `PhoneNumber`, `City`, `Keyword`, `DateAdded`, `Source`) VALUES (%s, %s, %s, %s, %s, %s) "
            # token_index = 0
            for location in input_data[0]:
                records = get_query_by_location(input_data[1], location)
                # save_data(rows)
                # is_first = False
                try:
                    cursor.executemany(insert_qry, records)
                    connection.commit()
                except mysql.connector.Error as error:
                    print("\nInsert Data invalid in location {} with {}".format(location, error))
                # break

    except mysql.connector.Error as error:
        print("\nFailed to insert record into MySQL table {}".format(error))

    finally:
        if connection is not None and connection.is_connected():
            if cursor is not None:
                cursor.close()
            connection.close()
        print('\nFinished successfully!')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("Started. Please wait a few minutes... \n")
    main()
