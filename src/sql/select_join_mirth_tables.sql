SELECT * FROM mirthdb.d_m101 LIMIT 100; -- server_id, received_date, processed, original_id, import_id, import_channel_id,  
SELECT * FROM mirthdb.d_mc101 LIMIT 100; -- metadata_id, message_id, content_type, content, is_encrypted, data_type 
SELECT * FROM mirthdb.d_mm101 LIMIT 100; -- id, message_id, server_id, received_date, status, connector_name, send_attempts, send_date, response_date, error_code, chain_id, order_id 
SELECT * FROM mirthdb.d_ms101 LIMIT 100; -- metadata_id, server_id, received, filtered, filtered_lifetime, sent, sent_lifetime, error, error_lifetime 
SELECT * FROM mirthdb.d_msq101 LIMIT 100; -- id
SELECT * FROM mirthdb.d_mcm101 LIMIT 100; -- metadata_id, message_id, source, type, patient_id, patient_state, order_type, order_state, collected_date, msg_ctrl_id, order_number

SELECT 'custom_metadata', custom_metadata.MESSAGE_ID, custom_metadata.TYPE, custom_metadata.PATIENTID, custom_metadata.ACCESSION, custom_metadata.ORDERTYPE, custom_metadata.MSGCTRLID, 
'metadata', metadata.RECEIVED_DATE, metadata.`STATUS`, metadata.SEND_DATE, metadata.RESPONSE_DATE 
'statistics', statistics.METADATA_ID, statistics.RECEIVED, statistics.FILTERED, statistics.SENT, statistics.ERROR,
'content', content.CONTENT_TYPE, content.CONTENT, content.DATA_TYPE
FROM mirthdb.d_mcm102 custom_metadata -- message metadata_id
INNER JOIN mirthdb.d_mm101 metadata ON metadata.MESSAGE_ID= custom_metadata.MESSAGE_ID
INNER JOIN mirthdb.d_ms101 statistics ON statistics.METADATA_ID= custom_metadata.METADATA_ID -- message received, filtered, sent
INNER JOIN mirthdb.d_mc101 content ON content.MESSAGE_ID= metadata.MESSAGE_ID
LIMIT 100


SELECT 'custom_metadata', custom_metadata.MESSAGE_ID, custom_metadata.TYPE, custom_metadata.PATIENTID, custom_metadata.ACCESSION, custom_metadata.ORDERTYPE, custom_metadata.MSGCTRLID, 
'metadata', metadata.RECEIVED_DATE, metadata.`STATUS`, metadata.SEND_DATE, metadata.RESPONSE_DATE 
'statistics', statistics.METADATA_ID, statistics.RECEIVED, statistics.FILTERED, statistics.SENT, statistics.ERROR,
'content', content.CONTENT_TYPE, content.CONTENT, content.DATA_TYPE
FROM mirthdb.d_mcm102 custom_metadata -- message metadata_id
INNER JOIN mirthdb.d_mm101 metadata ON metadata.MESSAGE_ID= custom_metadata.MESSAGE_ID
INNER JOIN mirthdb.d_ms101 statistics ON statistics.METADATA_ID= custom_metadata.METADATA_ID -- message received, filtered, sent
INNER JOIN mirthdb.d_mc101 content ON content.MESSAGE_ID= metadata.MESSAGE_ID
LIMIT 100

SELECT * FROM mirthdb.channel; -- ID, NAME, REVISION, CHANNEL 
SELECT * FROM mirthdb.d_channels; -- LOCAL_CHANNEL_ID, CHANNEL_ID
SELECT * FROM mirthdb.channel_group; -- ID, NAME, REVISION, CHANNEL_GROUP

-- DROP TABLE mirthdb.channel_id_tables
CREATE TABLE IF NOT EXISTS channel_id_tables (
 	RECORD_ID INT NOT NULL AUTO_INCREMENT,
	ID CHAR(36),
	CHANNEL_ID CHAR(36),
    CHANNEL_NAME VARCHAR(65),
	CONTENT VARCHAR(40),
	STATISTICS VARCHAR(40),
	CUSTOM_METADATA VARCHAR(40),
	METADATA VARCHAR(40),
	QUEUE VARCHAR(40),
	CREATED_DATE TIMESTAMP,
	INSERTED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (RECORD_ID)
)

-- SELECT * FROM mirthdb.channel_id_tables
DROP TABLE channel_id_tables

INSERT INTO channel_id_tables (ID, CHANNEL_ID, CHANNEL_NAME, CONTENT, STATISTICS, CUSTOM_METADATA, METADATA, QUEUE, CREATED_DATE) 
SELECT A.ID, B.LOCAL_CHANNEL_ID, A.NAME, CONCAT('d_mc',B.LOCAL_CHANNEL_ID), CONCAT('d_ms',B.LOCAL_CHANNEL_ID), CONCAT('d_mcm',B.LOCAL_CHANNEL_ID), CONCAT('d_mm',B.LOCAL_CHANNEL_ID), CONCAT('d_msq',B.LOCAL_CHANNEL_ID), CURRENT_TIMESTAMP() 
FROM mirthdb.channel A
INNER JOIN mirthdb.d_channels B ON B.CHANNEL_ID= A.ID
ORDER BY A.ID

SELECT * FROM mirthdb.channel_id_tables WHERE channel_name LIKE '%State Reporting%'

BEGIN

DECLARE @channel_name VARCHAR(30), @channel_id INT, @content VARCHAR(30), @statistics VARCHAR(30), @custom_metadata VARCHAR(30), @metadata VARCHAR(30);

SELECT channel_name, channel_id, content, statistics, custom_metadata, metadata INTO @channel_name, @channel_id, @content, @statistics, @custom_metadata, @metadata FROM mirthdb.channel_id_tables WHERE channel_name LIKE '%State Reporting%';

SELECT @channel_name, @channel_id, @content, @statistics, @custom_metadata, @metadata;

-- SET @statement = CONCAT('SELECT * FROM mirthdb.', @content, ' where ID = ', @ID_1); 
SET @statement = CONCAT('SELECT COUNT(message_id) FROM mirthdb.', @content); 

SET @statement = CONCAT('SELECT custom_metadata.MESSAGE_ID, custom_metadata.TYPE, custom_metadata.PATIENTID, custom_metadata.ACCESSION, custom_metadata.ORDERTYPE, custom_metadata.MSGCTRLID, metadata.RECEIVED_DATE, metadata.STATUS, metadata.SEND_DATE, metadata.RESPONSE_DATE, statistics.METADATA_ID, statistics.RECEIVED, statistics.FILTERED, statistics.SENT, statistics.ERROR, content.CONTENT_TYPE, content.DATA_TYPE
FROM mirthdb.',@custom_metadata,' custom_metadata 
INNER JOIN mirthdb.',@metadata,' metadata ON metadata.MESSAGE_ID= custom_metadata.MESSAGE_ID
INNER JOIN mirthdb.',@statistics,' statistics ON statistics.METADATA_ID= custom_metadata.METADATA_ID 
INNER JOIN mirthdb.',@content,' content ON content.MESSAGE_ID= metadata.MESSAGE_ID
LIMIT 100'

SELECT @statement;

PREPARE stmt1 FROM @statement; 
EXECUTE stmt1; 
DEALLOCATE PREPARE stmt1; 

END;

