SELECT  
	"area", 
	"eCAP_Area", 
	"template_name", 
	"ch_id", 
	"ch_name", 
	"design_id", 
	"lot_id", 
	"PARAMETER_NAME", 
	"process_step_name", 
	"process_tool_name",
	"process_tool_type", 
	"process_position", 
	"process_recipe", 
	"instance_id", 
	"STATUS",
	"instance_type", 
	"observationStartTime",
	Null AS "observation_end_time",
	Null AS	"operator", 
	Null AS	"metro_tool_name", 
	Null AS	"wafer_id", 
	Null AS	"special_flags",
	Null AS	"metro_step_name", 
	Null AS	"chart_type",
	Null AS	"violations",
	Null AS	"root_cause", 
	Null AS	"rc", 
	Null AS	"rc_charting", 
	Null AS	"rc_equip", 
	Null AS	"rc_incoming", 
	Null AS	"rc_metro", 
	Null AS	"rc_process", 
	Null AS	"rc_none", 
	Null AS	"rc_swr", 
	Null AS	"ecap_area", 
	Null AS	"impacted_level", 
	Null AS	"impacting_area", 
	Null AS	"impacting_step", 
	Null AS	"impacted_tool", 
	Null AS	"dts_string", 
	Null AS	"remeasure", 
	Null AS	"minToClose", 
	"WW" FROM "GAYUS"."F10N_eCAP_DAM_View"

	UNION
	
	SELECT 
		"area", 
		Null AS "eCAP_Area",
		"ecap_template_name" AS "template_name", 
		"ch_id", 
		Null AS "ch_name",
		"design_id",
		"lot_id",
		"PARAMETER_NAME",
		"process_step_name", 
		"process_tool_name",
		"process_tool_type", 
		"process_position", 
		Null AS "process_recipe",
		"instance_id", 
		Null as "STATUS",
		"instance_type",
		Null AS "observationStartTime",
		"observation_end_time", 
		"operator", 
		"metro_tool_name", 
		"wafer_id", 
		"special_flags",
		"metro_step_name", 
		"chart_type",
		"violations",
		"root_cause", 
		"rc", 
		"rc_charting", 
		"rc_equip", 
		"rc_incoming", 
		"rc_metro", 
		"rc_process", 
		"rc_none", 
		"rc_swr", 
		"ecap_area", 
		"impacted_level", 
		"impacting_area", 
		"impacting_step", 
		"impacted_tool", 
		"dts_string", 
		"remeasure", 
		"minToClose", 
		"ww" AS "WW"
		 FROM "GAYUS"."F10N_eCAP_Historic_View";