-- This script converts the json string to json object for the edge table
-- Run with:
-- mysql -h <jobmondb host> -P 3306 -u <service_user> -p <jobmondb> < convert_jsonstr_json.sql

DELIMITER //

CREATE PROCEDURE convert_json_strings()
BEGIN
    DECLARE rows_affected INT DEFAULT 1;

    -- Loop for upstream_node_ids
    WHILE rows_affected > 0 DO
        UPDATE edge
        SET upstream_node_ids = CAST(JSON_UNQUOTE(upstream_node_ids) AS JSON)
        WHERE JSON_TYPE(upstream_node_ids) = 'STRING'
        LIMIT 100000;
        SET rows_affected = ROW_COUNT();
    END WHILE;

    SET rows_affected = 1;

    -- Loop for downstream_node_ids
    WHILE rows_affected > 0 DO
        UPDATE edge
        SET downstream_node_ids = CAST(JSON_UNQUOTE(downstream_node_ids) AS JSON)
        WHERE JSON_TYPE(downstream_node_ids) = 'STRING'
        LIMIT 100000;
        SET rows_affected = ROW_COUNT();
    END WHILE;
END;
//

DELIMITER ;

LOCK TABLES edge WRITE;
CALL convert_json_strings();
UNLOCK TABLES;

DROP PROCEDURE IF EXISTS convert_json_strings;