REPLACE VIEW GAYUS.F10N_eCAP_COMBINED_View AS

SELECT	
	COALESCE(h.area,d.area) as area, /*selects first non-null value from h.area or d.area; if both null, null value*/
	COALESCE(h.ecap_area,d.eCAP_Area) as eCAP_Area,
	COALESCE(h.ecap_template_name,d.template_name) as template_name,
	COALESCE(h.WW,d.WW) as WW,
	COALESCE(h.ch_id,d.ch_id) as ch_id,
	d.ch_name,
	COALESCE(h.design_id,d.design_id) as design_id,
	COALESCE(h.lot_id,d.lot_id) as lot_id,
	COALESCE(h.parameter_name,d.parameter_name) as parameter_name,
	d.step_name,
	COALESCE(h.process_tool_name,d.process_tool_name) as process_tool_name,
	COALESCE(h.process_tool_type,d.process_tool_type) as process_tool_type,
	COALESCE(h.process_position,d.process_position) as process_position,
	COALESCE(h.process_step_name,d.process_step_name) as process_step_name,
	d.process_recipe,
	COALESCE(h.instance_id,d.instance_id) as instance_id,
	d.status,
	d.observationStartTime,
	h.observation_end_time,
	h.operator,
	h.metro_tool_name,
	h.wafer_id,
	h.special_flags,
	h.metro_step_name,
	h.chart_type,
	h.violations,
	h.root_cause,
	h.rc,
	h.rc_charting,
	h.rc_equip,
	h.rc_incoming,
	h.rc_metro,
	h.rc_process,
	h.rc_none,
	h.rc_swr,
	h.impacted_level,
	h.impacting_area,
	h.impacting_step,
	h.impacted_tool,
	h.dts_string,
	h.remeasure,
	h.minToClose,
	Tool2.Workstation

FROM 
	(SELECT 
		 
	 	"area", "ww" as "WW", "ecap_template_name", "observation_end_time", "instance_id", "ch_id", "operator", "PARAMETER_NAME" as "parameter_name", 
	 	"process_tool_name", "process_position", "metro_tool_name", "lot_id", "design_id", "wafer_id", "special_flags", "process_step_name", "metro_step_name", 
	 	"process_tool_type", "chart_type", "violations", "root_cause", "rc", "rc_charting", "rc_equip", "rc_incoming", "rc_metro", "rc_process", "rc_none", 
	 	"rc_swr", "ecap_area", "impacted_level", "impacting_area", "impacting_step", "impacted_tool", "dts_string", "remeasure", "minToClose" 
	 
	FROM "GAYUS"."F10N_eCAP_Historic_View"
	WHERE ecap_template_name LIKE '%DF%') as h
	
	FULL JOIN 
	
	(SELECT 
 
		"area", "eCAP_Area", "WW", "template_name", "ch_id", "ch_name", "design_id", "lot_id", "PARAMETER_NAME" as "parameter_name", "step_name", 
		"process_tool_name", "process_tool_type", "process_position", "process_step_name", "process_recipe", "instance_id", "STATUS" as "status", 
		"observationStartTime"
		
	FROM "GAYUS"."F10N_eCAP_DAM_View"
	WHERE "template_name" LIKE '%DF%' AND "status" = 'P' OR "template_name" LIKE '%DF%' AND "status" = 'C' OR "template_name" LIKE '%DF%' AND "status" = 'A'
	/*filter out status=NA*/) as d
	ON (h.ecap_template_name=d.template_name AND h.area=d.area AND h.WW=d.WW AND h.ch_id=d.ch_id AND h.parameter_name=d.parameter_name 
		AND h.process_tool_name=d.process_tool_name AND h.process_position=d.process_position AND h.lot_id=d.lot_id AND h.design_id=d.design_id 
		AND h.process_step_name=d.process_step_name AND h.process_tool_type=d.process_tool_type AND h.instance_id=d.instance_id AND h.ecap_area=d.eCAP_Area)
	
	LEFT JOIN
		
	(SELECT 
 	Tool.equip_id, 
 	Tool.WS_name as Workstation
 	FROM
 		(SELECT 

		RTRIM(AREA.mfg_area_id) AS mfg_area_id,
		RTRIM(EQUIP.equip_type_id) AS equip_type_id,
		RTRIM(EQUIP.equip_id) AS equip_id,
		RTRIM(WS.WS_name) AS WS_name,
		(CASE ESAREA.semi_state_id WHEN 'UNSCHEDULED_DOWNTIME' THEN 'UNSCHED_DOWN' WHEN 'SCHEDULED_DOWNTIME' THEN 'SCHED_DOWN' ELSE RTRIM(ESAREA.semi_state_id) END) AS semi_state_id,
		RTRIM(ESAREA.equip_state_id) AS equip_state_id,
		(ROUND(((CAST(((CURRENT_TIMESTAMP - EQSTAT.current_equip_state_datetime) DAY(4)) AS VARCHAR(20)))/24),1)) Time_Difference_in_Hours_from_CurrentTime,
		(CAST((SUBSTR((TO_CHAR(EQSTAT.current_equip_state_datetime)),1,10)) AS DATE) - DATE '1899-12-30') Time_Difference_in_Days_from_30121899, /*what are these columns for??*/
		(CAST((SUBSTR((TO_CHAR(EQSTAT.current_equip_state_datetime)),12,8)) AS TIME(0))) Time_of_Eqp_St_Datetime,
		RTRIM(EQUIP.equip_desc) AS equip_desc,
		RTRIM(EQUIP.physical_location) AS physical_location,
		(CASE WHEN SAP.micron_username IS NULL THEN 
			CASE EQSTAT.current_equip_state_emp_no WHEN 0 THEN '* AUTO TOOL *' ELSE CAST(EQSTAT.current_equip_state_emp_no AS CHAR) END
		WHEN SAP.micron_username='' THEN CAST(EQSTAT.current_equip_state_emp_no AS CHAR) 
		ELSE RTRIM(SAP.micron_username) END) micron_username,
		RTRIM(EQSTAT.current_equip_state_by_system) AS current_equip_state_by_system,
		NOTE.event_note_text AS event_note_text
		
		FROM
		
		FAB_10_ET_DM.EQUIPMENT EQUIP /*(equip_tracking_DSS..equipment)*/
		INNER JOIN FAB_10_REF_DM.FP_EQUIP FPE /*(reference..FP_equip)*/
		ON EQUIP.equip_OID = FPE.equip_OID
		LEFT OUTER JOIN FAB_10_REF_DM.FP_WS WS /*(reference..FP_WS)*/
		ON FPE.WS_OID = WS.WS_OID
		INNER JOIN FAB_10_ET_DM.CURRENT_EQUIPMENT_STATE EQSTAT /*(equip_tracking_DSS..current_equipment_state)*/
		ON EQUIP.equip_OID = EQSTAT.equip_OID
		INNER JOIN FAB_10_ET_DM.EQUIPMENT_STATE_FOR_AREA ESAREA /*(equip_tracking_DSS..equipment_state_for_area)*/
		ON EQSTAT.equip_state_OID = ESAREA.equip_state_OID
		INNER JOIN FAB_10_REF_DM.MFG_AREA AREA /*(reference..mfg_area)*/ 
		ON EQUIP.mfg_area_OID = AREA.mfg_area_OID
		LEFT OUTER JOIN FAB_10_REF_DM.SAP_WORKER SAP /*(reference..SAP_worker)*/
		ON EQSTAT.current_equip_state_emp_no = SAP.worker_no
		LEFT OUTER JOIN FAB_10_ET_DM.EVENT_NOTE NOTE /*(equip_tracking_DSS..event_note)*/
		ON EQSTAT.production_state_change_OID = NOTE.production_state_change_OID
		
		WHERE
		EQUIP.mfg_facility_OID = '8E3074D7400A9854'XBV AND
		EQUIP.equip_status = 'ACTIVE' AND
		EQUIP.equip_type_id NOT IN ('SCRUBBER','PUMP','CHILLER','LSC','TAP TEST','TRAINING','LOADPORT','LOADPORTNNC') 
		AND EQUIP.equip_type_id NOT LIKE 'FOUP%' AND EQUIP.equip_type_id NOT LIKE 'Z%' /*Z%? No Z in this column */ 
		AND AREA.mfg_area_id LIKE '%DIFFUSION%') AS Tool
	
		WHERE SUBSTR(Tool.equip_id,9,2)='00') AS Tool2
		
	ON Tool2.equip_id = COALESCE(h.process_tool_name,d.process_tool_name)
	WHERE Tool2.Workstation IS NOT NULL;