# coding: utf8

# ################################################## IMPORT ##################################################

import ets.etsMysqlConnector as etsMysqlConnector
import ets.etsConfigParser as etsConfigParser
import queries

# ################################################## PARAMETERS ##################################################

mysqlConfigFile = 'C:/Users/belim/PycharmProjects/CONFIG/mysql.conf'

GROUP_CONCAT_SEPARATOR = ','    # разделитель group_concat используемый в запросах, по умолчанию ','

# ################################################## CONFIG PARSE ##################################################

mysqlConfig = etsConfigParser.ConfigParser(configFile=mysqlConfigFile)

# Конфигурация подключения SQL
sqlData44_1 = mysqlConfig.getOptionsFromSection('sql44-1')

sqlUser44_1 = sqlData44_1['sqluser']
sqlPassword44_1 = sqlData44_1['sqlpassword']
sqlHost44_1 = sqlData44_1['sqlhost']
sqlDatabase44_1 = sqlData44_1['sqldatabase']
sqlPort44_1 = sqlData44_1['sqlport']

# ################################################## FUNCTIONS ##################################################


# функция записи данных в бд
def writeInTable():
    if blockId in notCreatedBlockIds:
        sql44сonnection.executeQuery(querry=queries.insertQuery % {'blockId': blockId, 'unlockId': unlockId})
        print(blockId, unlockId)


# функция проверки эквивалентности двух значений
def isEqual(x, y): return x == y

# ################################################## CODE ##################################################

# инициализация sql подключений
sql44сonnection = etsMysqlConnector.MysqlConnection(sqlUser=sqlUser44_1,
                                                    sqlPassword=sqlPassword44_1,
                                                    sqlHost=sqlHost44_1,
                                                    sqlDatabase=sqlDatabase44_1,
                                                    sqlPort=sqlPort44_1)

# отправляем в архив старые транзакции
sql44сonnection.executeQuery(querry=queries.archiveOldTransaction)
# получаем набор данных из БД
transactionIdsData = sql44сonnection.executeQuery(querry=queries.getTransactionIdsQuery)

# получаем список отсутствующих в таблице id блокировок
notCreatedBlockIds = sql44сonnection.executeQuery(querry=queries.getNotCreatedBlockIdsQuery)
notCreatedBlockIds = [int(notCreatedBlockId[0]) for notCreatedBlockId in notCreatedBlockIds]

# обрабатывем данные построчно
for transactionLine in transactionIdsData:

    # список id блокировок (integer)
    blockIds = [int(blockId) for blockId in transactionLine[2].split(GROUP_CONCAT_SEPARATOR)]
    # список дат блокировок (string)
    blockDates = transactionLine[3].split(GROUP_CONCAT_SEPARATOR)
    # если найдены какие-либо разблокировки, то их обработаем аналогично
    # если не найдены, то вернем False
    if transactionLine[4]:
        # список id разблокировок (integer)
        unlockIds = [int(unlockId) for unlockId in transactionLine[6].split(GROUP_CONCAT_SEPARATOR)]
        # список дат разблокировок (string)
        unlockDates = transactionLine[7].split(GROUP_CONCAT_SEPARATOR)
    else:
        unlockIds = False
        unlockDates = False

    # проверка равенства разблокированных средств заблокированным
    AMOUNT_EQUAL = isEqual(transactionLine[1], transactionLine[5])
    # проверка равенства количества блокировок и разблокировок
    TRANSACT_COUNT_EQUAL = isEqual(transactionLine[0], transactionLine[4])

    # если деньги разблокированы и количество транзакций сошлось
    if AMOUNT_EQUAL and TRANSACT_COUNT_EQUAL:
        for k in range(int(transactionLine[0])):
            blockId = blockIds[k]
            unlockId = unlockIds[k]
            writeInTable()
        continue

    # теперь разложим по полочкам все оставшиеся
    # инициализируем переменную-метку того, что найдена соответствующая блокировке разблокировка
    IS_UNLOCKED = False
    # для каждой блокировки
    while True:
        # если осталась хоть одна разблокировка, то начинаем проверять
        # если нет, то считаем, что для данной блокировки больше разблокировок нет и проверять больше нечего
        # текущую разблокировку пишем как неразблокированную только если не установлен IS_UNLOCKED
        # все последующие однозначно пишем как неразблокированные
        if unlockIds:

            # проверим, что заблокировано раньше, чем разблокировано
            # если это не так, значит текущая разблокировка относится к более ранней блокировке
            # следует перейти к следующей разблокировке
            if blockDates[0] < unlockDates[0]:

                # сравним дату следующей блокировки с текущей разблокировкой
                try:
                    otherBlockUnlock = blockDates[1] < unlockDates[0]
                # если же новее блокировок нет, значит текущая и все последующие разблокировки
                # точно относится к текущей блокировке
                # причем на этом можно завершать проверку
                except IndexError:
                    blockId = blockIds[0]
                    unlockId = unlockIds[0]
                    writeInTable()
                    IS_UNLOCKED = True
                    break

                # проверим, если дата следующей по порядку блокировки меньше чем дата текущей разблокировки
                # значит разблокировка относится к ней и по текущей блокировке больше нет разблокировок
                # текущую можно убирать и переходить к следующей блокировке
                # НО ХОТЯ БЫ ОДНА РАЗБЛОКИРОВКА ДОЛЖНА БЫТЬ ПРИВЯЗАНА ДАЖЕ ПРОТИВ ЛОГИКИ!
                # В БД КАК МИНИМУМ 41 СЛУЧАЙ КРИВЫХ БЛОКИРОВОК-РАЗБЛОКИРОВОК!
                if otherBlockUnlock:
                    if not IS_UNLOCKED:
                        blockId = blockIds.pop(0)
                        unlockId = unlockIds.pop(0)
                        writeInTable()
                        blockDates.pop(0)
                        unlockDates.pop(0)
                        IS_UNLOCKED = True
                    else:
                        blockIds.pop(0)
                        blockDates.pop(0)
                    IS_UNLOCKED = False
                    continue

                # если же нет, то текущая разблокировка точно относится к текущей блокировке
                # и есть смысл проверить принадлежность следующей разблокировки к текущей блокировке
                else:
                    # нас интересует только первая разблокировка к каждой блокировке
                    # поэтому если стоит метка IS_UNLOCKED, то просто отбрасываем текущую разблокировку
                    if IS_UNLOCKED:
                        unlockId = unlockIds.pop(0)
                        unlockDates.pop(0)
                    # если же это первая разблокировка, то заносим ее в БД
                    else:
                        blockId = blockIds[0]
                        unlockId = unlockIds.pop(0)
                        unlockDates.pop(0)
                        writeInTable()
                        IS_UNLOCKED = True
                    continue

            unlockIds.pop(0)
            unlockDates.pop(0)
            IS_UNLOCKED = False

        else:
            if IS_UNLOCKED:
                blockIds.pop(0)
                blockDates.pop(0)
            for x in range(len(blockIds)):
                blockId = blockIds[x]
                unlockId = 0  # в запрос добавим несуществующий id, тогда в выборке по разблокировке вернутся null
                writeInTable()
            IS_UNLOCKED = False
            break

# закрываем соединение
sql44сonnection.disconnect()
exit(0)
