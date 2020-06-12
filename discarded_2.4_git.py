#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""  ******************************************************************************
     ************** Скрипт поиска в папке discarded "Заявка пользователя" *********
     ************** и поиска каталогов с файлами формата doc, txt         *********
     ******************************************************************************
"""

import os
import time
import zipfile
import logging
import sqlite3
from sqlite3 import Error
from pathlib import Path

## ''' *********** Настраиваемые параметры ************** '''

logging.basicConfig(filename=r"discarded.log",
                    format="%(asctime)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
                    level=logging.INFO)

pathDis     = r'PATH_TO\discarded'          # Путь до каталога discarded, где осуществляется поиск
pathDisTmp  = r'PATH_TO\discarded_temp'     # Путь до каталога, куда складываются архивы по датам
pathDb      = r"PATH_TO\dbdiscarded.db"     # Путь до БД
pattern     = 'Заявка пользователя'         # Переменная, по которой ищутся папки        
listSwitch  = ['.doc', '.txt']              # Список расширений файлов, которые присутствуют в discarded
dtimer      = 2                             # Выборка папок из discarded старше указанных дней
day         = 7                             # Удалить строки из dbdiscarded.db старше указанных дней

## ''' ************************************************* '''


def check_dir_and_files():
    """ Проверяем каталог discarded на наличие папок за dtimer дней.
    Возвращаем списки listDirs (root, files, timedir, dirtype) """

    listDirs =  []
    for root, dirs, files in os.walk(pathDis):
        timedir = os.path.getmtime(root)
        if timedir >= time.time() - 60*60*24*dtimer:
            if pattern in os.path.basename(root):
                dirtype = 'op'
                listDirs.append([root, files, timedir, dirtype])
            else:
                for f in files:
                    for f_ in listSwitch:
                        if Path(f).suffix == f_:
                            dirtype = 'other'
                            listDirs.append([root, files, timedir, dirtype])
    return listDirs


def copy_and_zip(listDirsNew):
    """ Создаем папку timeDir с сегодняшней датой, куда складываем zip архивы из 
    списка listDirsNew. Возвращаем список listDZip из путей созданных архивов """

    timeMD = time.strftime("%m-%d", time.localtime())
    timeY = time.strftime("%Y", time.localtime())
    timeDir = os.path.join(pathDisTmp, timeY, timeMD)

    if os.path.exists(timeDir) is False:
        os.makedirs(timeDir)
        print(f'Создал папку: {timeDir}')
        logging.info(f'Создал папку: {timeDir}')
        while os.path.isdir(timeDir) is False:
            time.sleep(1)

    listDZip = []
    for d in listDirsNew:
        root, files, timedir, dirtype = d[0], d[1], d[2], d[3]

        basename = os.path.basename(root)
        pathBasename = os.path.join(timeDir, basename + '.zip')
        with zipfile.ZipFile(f'{pathBasename}', 'w') as myzip:
            os.chdir(pathDis)
            myzip.write(basename)
            for i in files:
                myzip.write(os.path.join(basename, i))
            listDZip.append([basename, pathBasename, timedir, dirtype])

    logging.info(f'Новые zip-архивы: {listDZip}')
    return listDZip


def sql_connection():
    """ Пытаемся подключиться к БД, возвращаем conn """

    try:
        conn = sqlite3.connect(pathDb)
        return conn
    except Error:
        print(Error)


def del_old_dir_db(conn):
    """ Удаляем старые папки из БД, которые старше day дней """

    cursor = conn.cursor()

    t = time.time() - (60*60*24*day)  # Кол-во дней по времени
    sql = "DELETE FROM discarded where time < ?"
    deleted = cursor.execute(sql, ([t]))
    print(f'Удалено старых строк из БД: {deleted.rowcount}')
    logging.info(f'Удалено старых строк из БД: {deleted.rowcount}')
    conn.commit()


def check_dir_db(conn):
    """ Возвращаем список каталогов dirDb из БД """

    cursor = conn.cursor()

    sqldir = "SELECT dir FROM discarded"
    cursor.execute(sqldir)
    dirDbAll = cursor.fetchall()
    # print('dirDbAll', dirDbAll)
    dirDb = []
    for i in dirDbAll:
        dirDb.append(i[0])
    return dirDb


def get_new_dir(listDirs, dirDb):
    """ Сравниваем списки с каталогами из БД (dirDb) и из найденных в каталоге 
    discarded (listDirs). Возвращаем отредактированный список listDirs убрав из 
    него элементы с совпадающими названиями каталогов из БД """

    # Берем только название папок
    listDirsName = [os.path.basename(x[0]) for x in listDirs]
    # Генерируем список с совпадениями в списках listDZip, dirDb
    result = set(dirDb).intersection(listDirsName)

    for l in result:
        for ll in listDirs:
            if l == os.path.basename(ll[0]):
                listDirs.remove(ll)
    logging.info(f'Возвращаю отсортированный список каталогов: {listDirs}')
    return listDirs


def write_new_dir_db(conn, listDZip):
    """ Записываем в БД элементы списка zip архива из listDZip """

    cursor = conn.cursor()

    for z in listDZip:
        basename, pathBasename, timedir, dirtype = z[0], z[1], z[2], z[3]

        sql = """
        INSERT INTO discarded VALUES
        (null,?,?,?,?)
        """
        cursor.executemany(sql, [(basename, pathBasename, timedir, dirtype)])

    conn.commit()
    logging.info(f'Записал новые zip архивы в БД: {listDZip}')


def send_zip_chat(countZip):
    """ Отправляем собранные данные в чат MATTERMOST """
    import requests
    import json
    from urllib.request import Request, urlopen
    from urllib.error import URLError, HTTPError

    req = Request("http://HOST_MATTERMOST/")
    countZipText = f'Создал {countZip} zip архивов'
    try:
        logging.info(
            f'Проверяю доступность чата')
        urlopen(req)
    except HTTPError as e:
        err = f'Error code: {e.code}'
        logging.error(f'Ошибка send_zip_chat() {err}')
        print(err)
        send_zip_thebat(countZipText, err)
    except URLError as e:
        err = 'Error code: {}'.format(e.reason)
        logging.error(f'Ошибка send_zip_chat() {err}')
        print(err)
        send_zip_thebat(countZipText, err)
    except Exception as e:
        err = 'Error: {}'.format(e.args)
        logging.error(f'Ошибка send_zip_chat() {err}')
        print(err)
        send_zip_thebat(countZipText, err)
    else:
        # print('good!')

        URL = 'http://HOST_MATTERMOST/hooks/0000011111222233333'
        payload = {"channel": "CHANNEL_MATTERMOST",
                   "username": "DISCARDED-bot",
                   "icon_url": "https://cdn.pixabay.com/photo/2017/10/24/00/39/bot-icon-2883144_640.png",
                   "text": f"### {countZipText}"}
        requests.post(URL, data=json.dumps(payload))
        logging.info(
            f'Отправил в чат: {countZipText}')


def send_zip_thebat(listDZip):
    """ В ДОРАБОТКЕ
    Отправляем по THE BAT архивы"""

    if not listDZip:
        countZip = 0
    else:
        countZip = len(listDZip)

    print(f'Отправляю по бату: {countZip} архивов')
    for z in listDZip:
        if z[-1] == 'op':
            print(f'Отправил {z[1]} на op@mail.ru')
        elif z[-1] == 'other':
            print(f'Отправил {z[1]} на other@mail.ru')
        else:
            print(f'Не разобрался куда отправить {z[1]}')
        

def main():
    listDirs = check_dir_and_files() # Проходим по папкам в каталоге
    
    conn = sql_connection()  # Подключаемся к БД (Таблица discarded состоит из id,dir,link,time.dirtype)

    del_old_dir_db(conn)  # Удаляем старые row и возвращаем весь список папок
    dirDb = check_dir_db(conn) # Читаем из БД имена каталогов для сравнения
    listDirsNew = get_new_dir(listDirs, dirDb)  # Сверяем несовпадающие имена папок в списках
    listDZip = copy_and_zip(listDirsNew)  # Создаем архив из папок
    write_new_dir_db(conn, listDZip)  # Пишем созданные ZIP в БД
    
    conn.close # Отключаемся от БД

    if not listDZip:
        pass
    else:
        countZip = len(listDZip)
        send_zip_chat(countZip)  # Отправляем данные в чат по вебхуку

    send_zip_thebat(listDZip)  # Отправляем данные по THE BAT (В ДОРАБОТКЕ)


if __name__ == "__main__":
    main()
