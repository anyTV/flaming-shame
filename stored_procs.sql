
DROP PROCEDURE IF EXISTS GET_SEED;
DELIMITER $$
create procedure GET_SEED()
BEGIN
	DECLARE _index INT;
	DECLARE counter INT DEFAULT 0;

	looper : LOOP
		SET counter = counter + 1;
		SET _index = FLOOR(RAND() * (100000)) + 1;
		INSERT INTO temp (SELECT id FROM channels LIMIT _index, 1);
		IF counter < 50 THEN
			ITERATE looper;
		END IF;
		LEAVE looper;
	END LOOP looper;

	SELECT * FROM temp;
	DELETE FROM temp;

END $$
DELIMITER ;
CALL GET_SEED();

