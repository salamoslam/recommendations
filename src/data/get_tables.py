from ftplib import FTP
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import certifi
import os


with open('mongodb_pass.txt', 'r') as file:
    path2 = file.read()
# print(path2)


path_to_repo = '/Users/kuznetsovnikita' #здесь можно указать путь до папки, где лежит репозиторий
path = path_to_repo + '/recommendations/data/raw'


client = MongoClient(path2, tlsCAFile=certifi.where())
current_db = client['spin_services']

heats = current_db['cs_cart_heats'].find()
cart = current_db['cs_cart_carts'].find()
users = current_db['сs_cart_users'].find()



pd.DataFrame(heats).to_csv(os.path.join(path,r'heats.csv'))
pd.DataFrame(cart).to_csv(os.path.join(path,r'cart.csv'))
pd.DataFrame(users).to_csv(os.path.join(path,r'users.csv'))

now = datetime.now()\
      # - datetime.timedelta(days=1) # если ошибка вечернего времени (обновляется только в 7 утра)
# прописываем данные для доступа
with open('ftp_creds.txt', 'r') as ftp:
    lines = ftp.readlines()

lines = [line.replace(' ', '').rstrip() for line in lines]
my_host = lines[0]
my_port = int(lines[1])
my_user = lines[2]
my_pass = lines[3]


# подключаемся к серверу
ftp = FTP()
ftp.connect(my_host, my_port)
ftp.login(my_user, my_pass)
# переходим в конкретную папку "tov"
ftp.cwd('tov')
# на всяк печатаем содержимое папки
# указываем имя нужного файла и имя создаваемого файла
# ftp_file_name = "21_04_27.xlsx"
ftp_file_name = now.strftime("%y_%m_%d") + ".xlsx"

local_file_name = os.path.join(path,r'goods.xlsx')
# local_file_dash = '/Users/user/PycharmProjects/Heroku_Dashboard/goods.xlsx'
# пишем файл с сервера в локальный файл
with open(local_file_name, 'wb') as local_file_name:
    ftp.retrbinary('retr ' + ftp_file_name, local_file_name.write)
# with open(local_file_dash, 'wb') as local_file_dash:
#     ftp.retrbinary('retr ' + ftp_file_name, local_file_dash.write)

# закрываем соединение
ftp.close()

vygruz = pd.read_excel('/Users/kuznetsovnikita/recommendations/data/raw/goods.xlsx',
                       converters={'Штрихкод': str})
vygruz = vygruz.drop_duplicates(subset='Штрихкод')
new_names = {'Штрихкод': 'id',
             'Статус': 'status',
             'Основание статуса ': 'reason',
             'Дата приемки': 'cumdate',
             'Дата продажи / возврата ': 'selldate',
             'Комитент': "comit",
             'Категория': 'categ',
             'Бренд': 'brand',
             'Состояние': 'cond',
             'Цена акта': 'actprice',
             'Цена продажи ': 'sellprice',
             'место хранения': 'storage'}

vygruz.rename(columns=new_names, inplace=True)
vygruz['selldate'] = pd.to_datetime(vygruz['selldate'], format='%d.%m.%Y')
vygruz['cumdate'] = pd.to_datetime(vygruz['cumdate'], format='%d.%m.%Y')
vygruz.to_excel(os.path.join(path,r'goods.xlsx'))
