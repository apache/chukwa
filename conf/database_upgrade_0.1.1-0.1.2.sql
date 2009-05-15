DELIMITER $$
DROP PROCEDURE IF EXISTS UpgradeTable$$
CREATE PROCEDURE UpgradeTable()
BEGIN
    DECLARE str VARCHAR(4000);
    DECLARE tname VARCHAR(255);
    DECLARE counter INT DEFAULT 0;
    DECLARE st CURSOR FOR SELECT t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_NAME LIKE 'mr_job_%' and t.TABLE_NAME!='mr_job_template';
    DECLARE cnt CURSOR FOR SELECT count(t.TABLE_NAME) FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_NAME LIKE 'mr_job_%' and t.TABLE_NAME!='mr_job_template';
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET counter = counter - 1;
    OPEN cnt;
    FETCH cnt INTO counter;
    CLOSE cnt;
    OPEN st;
    REPEAT
        FETCH st INTO tname;
        SET @str = CONCAT('ALTER IGNORE TABLE ',tname,' ADD COLUMN finished_maps bigint default 0, ADD COLUMN finished_reduces bigint default 0, ADD COLUMN failed_maps bigint default 0, ADD COLUMN failed_reduces bigint default 0, ADD COLUMN total_maps bigint default 0, ADD COLUMN total_reduces bigint default 0, ADD COLUMN reduce_shuffle_bytes bigint default 0;');
        SELECT tname;
        PREPARE stmt from @str;
        EXECUTE stmt;
        COMMIT;
        DEALLOCATE PREPARE stmt;	
        SET counter = counter - 1;
	UNTIL counter=0 END REPEAT;
    CLOSE st;
END$$
DELIMITER ;

call UpgradeTable();

DELIMITER $$
DROP PROCEDURE IF EXISTS UpgradeTable$$
CREATE PROCEDURE UpgradeTable()
BEGIN
    DECLARE str VARCHAR(4000);
    DECLARE tname VARCHAR(255);
    DECLARE counter INT DEFAULT 0;
    DECLARE st CURSOR FOR SELECT t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_NAME LIKE 'mr_task_%' and t.TABLE_NAME!='mr_task_template';
    DECLARE cnt CURSOR FOR SELECT count(t.TABLE_NAME) FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_NAME LIKE 'mr_task_%' and t.TABLE_NAME!='mr_task_template';
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET counter = counter - 1;
    OPEN cnt;
    FETCH cnt INTO counter;
    CLOSE cnt;
    OPEN st;
	REPEAT
        FETCH st INTO tname;
        SET @str = CONCAT('ALTER IGNORE TABLE ',tname,' ADD COLUMN type VARCHAR(20),ADD COLUMN reduce_shuffle_bytes bigint default 0,ADD COLUMN hostname VARCHAR(80),ADD COLUMN shuffle_finished timestamp default 0,ADD COLUMN sort_finished timestamp default 0,ADD COLUMN spilts bigint default 0;');
        SELECT tname;
        PREPARE stmt from @str;
        EXECUTE stmt;
        COMMIT;
        DEALLOCATE PREPARE stmt;
        SET counter = counter - 1;
        UNTIL counter=0 END REPEAT;
    CLOSE st;
END$$
DELIMITER ;

call UpgradeTable();
DROP PROCEDURE IF EXISTS UpgradeTable;
