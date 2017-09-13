getTransactionIdsQuery = '''SELECT
  bl.idCount AS 'Количество блокировок',
  bl.blSum AS 'Сумма блокировок',
  bl.blIds AS 'Id блокировок',
  bl.blDateTime AS 'Даты блокировок',
  un.idCount AS 'Количество разблокировок',
  un.ulSum AS 'Сумма разблокировок',
  un.ulIds AS 'Id разблокировок',
  un.ulDateTime AS 'Даты разблокировок'
FROM
  (
    SELECT
      lotId,
      senderRelationId,
      GROUP_CONCAT(id ORDER BY id) AS blIds,
      COUNT(id) idCount,
      GROUP_CONCAT(datetime ORDER BY id) blDateTime,
      SUM(amount) AS blSum
    FROM paymentTransaction
    WHERE operationTypeId IN (1, 12)
    -- AND datetime < DATE_FORMAT(NOW(), '%Y-%m-%d 00:00:00')
    AND datetime < '2017-09-07 23:59:59'
    GROUP BY lotId, senderRelationId
  ) bl
  LEFT
  JOIN
    (
      SELECT
        lotId,
        senderRelationId,
        GROUP_CONCAT(id ORDER BY id) AS ulIds,
        COUNT(id) idCount,
        GROUP_CONCAT(datetime ORDER BY id) ulDateTime,
        SUM(amount) AS ulSum
      FROM paymentTransaction
      WHERE operationTypeId IN (2, 3, 13, 14)
    --  AND DATE(datetime) BETWEEN DATE(DATE_SUB(NOW(), INTERVAL %(UPDATE_INTERVAL)s DAY)) AND NOW()
     AND DATE(datetime) < '2017-09-07 23:59:59'
      GROUP BY lotId,
               senderRelationId
    ) un ON bl.lotId = un.lotId AND bl.senderRelationId = un.senderRelationId
;'''

insertQuery = '''INSERT INTO tsdBlockTransaction_test (lotId
 , senderRelationId
 , block_id
 , block_amount
 , block_dateTime
 , block_operationTypeId
 , unlock_id
 , unlock_amount
 , unlock_dateTime
 , unlock_operationTypeId)
  SELECT
    bl.lotId,
    bl.senderRelationId,
    bl.id block_id,
    bl.amount block_amount,
    bl.datetime block_dateTime,
    bl.operationTypeId block_operationTypeId,
    un.id unlock_id,
    un.amount unlock_amount,
    un.datetime unlock_dateTime,
    un.operationTypeId unlock_operationTypeId
  FROM paymentTransaction bl
    LEFT JOIN paymentTransaction un
      ON bl.lotId = un.lotId AND bl.senderRelationId = un.senderRelationId AND un.id = %(unlockId)s
  WHERE bl.id = %(blockId)s
;'''

archiveOldTransaction = '''UPDATE tsdBlockTransaction_test
SET archive = 1
WHERE archive = 0
AND unlock_id IS NULL
;'''

getNotCreatedBlockIdsQuery = '''SELECT searchIdTable.id
  FROM (
SELECT transactions.id, created.block_id
  FROM
(SELECT t.id FROM paymentTransaction t WHERE t.operationTypeId IN (1, 12)) AS transactions
  LEFT JOIN
(SELECT btt.block_id FROM tsdBlockTransaction_test btt WHERE btt.archive = 0) AS created
  ON transactions.id = created.block_id
  HAVING created.block_id IS NULL
  ) AS searchIdTable
  ;'''