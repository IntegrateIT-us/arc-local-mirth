BEGIN

DECLARE @channel_name VARCHAR(30), @channel_id INT, @content VARCHAR(30), @statistics VARCHAR(30), @custom_metadata VARCHAR(30), @metadata VARCHAR(30);

SELECT channel_name, channel_id, content, statistics, custom_metadata, metadata INTO @channel_name, @channel_id, @content, @statistics, @custom_metadata, @metadata FROM mirthdb.channel_id_tables WHERE channel_name LIKE '%State Reporting%';

SELECT @channel_name, @channel_id, @content, @statistics, @custom_metadata, @metadata;

SET @statement = CONCAT('SELECT custom_metadata.MESSAGE_ID, metadata.CHAIN_ID, metadata.ORDER_ID, metadata.ID, custom_metadata.METADATA_ID, metadata.CONNECTOR_NAME, custom_metadata.MSG_CTRL_ID, custom_metadata.TYPE, custom_metadata.PATIENT_ID, custom_metadata.ORDER_TYPE, custom_metadata.ORDER_NUMBER, custom_metadata.COLLECTED_DATE, metadata.RECEIVED_DATE, metadata.STATUS, metadata.SEND_DATE, metadata.RESPONSE_DATE FROM mirthdb.',@custom_metadata,' custom_metadata INNER JOIN mirthdb.',@metadata,' metadata ON metadata.MESSAGE_ID= custom_metadata.MESSAGE_ID WHERE metadata.CHAIN_ID<>0 AND custom_metadata.MSG_CTRL_ID= "00163621" ORDER BY custom_metadata.MESSAGE_ID, metadata.CHAIN_ID, metadata.ORDER_ID, metadata.ID, custom_metadata.METADATA_ID');

SELECT @statement;

PREPARE stmt1 FROM @statement; 
EXECUTE stmt1; 

END;