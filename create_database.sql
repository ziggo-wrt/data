/* Create database in server */
CREATE SCHEMA frauds_db;

CREATE TABLE frauds_db.transactions (
`tran_id` INT NOT NULL,
`act_date` DATETIME NOT NULL,
`client_id` INT NOT NULL,
`card_id` INT NOT NULL,
`amount` FLOAT NOT NULL,
`use_chip` VARCHAR(100),
`merchant_id` INT NOT NULL,
`merchant_city` VARCHAR(100),
`merchant_state` VARCHAR(100),
`zip` FLOAT,
`mcc_id` INT NOT NULL,
`errors` VARCHAR(100),
`fraud_status` VARCHAR(10),
`mcc_detail` VARCHAR(100),
PRIMARY KEY (`tran_id`));

/* Recheck sample data in database */
SELECT *
FROM frauds_db.transactions;

/* Recheck dimension in database */
SELECT
	COUNT(*) AS SumRows
FROM frauds_db.transactions;
