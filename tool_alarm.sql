SELECT 
	isv.WS_group_name AS 'WS Group Name',
	isv.frame_equip_id AS 'Frame Equip ID',
	SUBSTRING(isv.issue_details,CHARINDEX('Step Name',isv.issue_details,1)+11,CHARINDEX('*',isv.issue_details,1)-2+1-(CHARINDEX('Step Name',isv.issue_details,1)+11)) AS 'Step Name',
	SUBSTRING(isv.issue_details,CHARINDEX('Lot ID',isv.issue_details,1)+8,11) AS 'Lot ID',
	(
	CASE 
		WHEN SUBSTRING(isv.default_text,1,7)='IS012-1' THEN NULL
		WHEN SUBSTRING(isv.default_text,1,7)='IS012-2' THEN SUBSTRING(isv.issue_details,CHARINDEX('Wafer ID',isv.issue_details,1)+10,7)
		WHEN SUBSTRING(isv.default_text,1,7)='IS012-3' THEN SUBSTRING(isv.issue_details,CHARINDEX('wafer_id',isv.issue_details,1)+10,7) 
		END) AS 'Wafer ID',
	SUBSTRING(isv.issue_details,11,CHARINDEX(';',isv.issue_details,1)-1-11+1) AS 'ETI State',
	(
	CASE 
		WHEN SUBSTRING(isv.default_text,1,7)='IS012-1' THEN 'LOT FAILED TO START'
		WHEN SUBSTRING(isv.default_text,1,7)='IS012-2' THEN 'WAFER ALARM'
		WHEN SUBSTRING(isv.default_text,1,7)='IS012-3' THEN 'UNKNOWN' 
		END) AS 'Alarm Category',
	DATEDIFF(minute,isv.issue_datetime,CURRENT_TIMESTAMP) as 'Minutes Since Alarm Detected'

	FROM [OPS_IMP_METRICS].[OI_METRICS].[v_ISVHistory_past1hours] isv --is this table too real time? seems like only contains data from the past hour
	WHERE isv.flag_level_id='1' AND SUBSTRING(isv.default_text,1,7) IN ('IS012-1','IS012-2','IS012-3') 
	AND isv.mfg_area_id LIKE 'F10%DIFF%';