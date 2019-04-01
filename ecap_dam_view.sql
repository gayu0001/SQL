CREATE VIEW  GAYUS.F10N_eCAP_DAM_View AS
SELECT  
	UPPER(area) as area
	, UPPER(eCAP_Area) AS eCAP_Area
	, WW.mfg_year_no || trim(WW.mfg_ww_no(format '99')) AS WW
	, template_name 
	, ch_id 
	, ch_name 
	, seq_num 
	, design_id 
	, lot_id 
	, parameter_name 
	, step_name 
	, process_tool_name 
	, process_tool_type 
	, process_position 
	, process_step_name 
	, process_recipe
	, instance_type
	, Impacted_Level 
	, Impacting_Area 
	, Impacting_Step 
	, Impacted_Tool
	, DTSString 
	, collectionType 
	, chartType 
	, instance_id 
	, status
	, lastNodeUpdate 
	, observationStartTime  
FROM
(
SELECT  
	s.cf_value_11 AS area  
	--, wwUpdate.mfg_year_ww_num AS WW -- returns micron WW for the update time
	, d.name_ AS template_name 
	, s.ch_id AS ch_id 
	, scd.ch_name As ch_name
	, s.seq_num 
	, s.exval_01 AS design_id 
	, s.exval_12 AS lot_id 
	, s.parameter_name 
	, s.exval_04 AS step_name 
	, s.exval_07 AS process_tool_name 
	, s.exval_08 AS process_tool_type 
	, s.exval_11 AS process_position 
	, s.exval_06 AS process_step_name 
	, s.exval_10 AS process_recipe 
	 , CASE 
		WHEN iv.Level IS NOT NULL
		  THEN 'RDA'
		WHEN POSITION('LOTDISPO' IN d.name_) > 01
		  THEN 'DISPO'
		ELSE 'SPACE'
		END AS instance_type
	, iv.eCAP_Area
	, iv.Level AS Impacted_Level 
	, iv.Area AS Impacting_Area 
	, iv.Step AS Impacting_Step 
	, iv.Tool AS Impacted_Tool
	, iv.DTSString 
	, s.cf_value_09 AS collectionType 
	, s.cf_value_10 AS chartType 
	, i.id_ AS instance_id 
	, tei.status
	, COALESCE(i.end_,l.leave_) AS lastNodeUpdate 
	, i.start_ AS observationStartTime  
FROM 
	FAB_10_ECAP_DM.JBPM_PROCESSINSTANCE i  
	INNER JOIN FAB_10_ECAP_DM.JBPM_PROCESSDEFINITION d  
	ON i.processdefinition_ = d.id_  
	INNER JOIN FAB_10_ECAP_DM.T_SPACE2ECAP s  
	ON i.id_ = s.instance_id -- to account for multiple wafers/parameters AND s.seq_num = 1
	INNER JOIN FAB_10_SPC_DM.T_CHANNEL_DEF scd
	ON s.ch_id = scd.ch_id
	LEFT JOIN FAB_10_ECAP_DM.JBPM_TOKEN t  
	ON i.id_ = t.processinstance_  
	LEFT JOIN FAB_10_ECAP_DM.JBPM_NODE n  
	ON t.node_ = n.id_  
	LEFT JOIN  
	(  
		select * from
		(
		select processinstance_,
		    max(case when name_ = 'eCAP_Area' then stringvalue_ end) eCAP_Area,
		    max(case when name_ = 'Level' then stringvalue_ end) "Level",
		    max(case when name_ = 'Area' then stringvalue_ end) Area,
			max(case when name_ = 'Step' then stringvalue_ end) Step,
			max(case when name_ = 'Tool' then stringvalue_ end) Tool,
			max(case when name_ = 'DTSString' then stringvalue_ end) DTSString
		from
		(  
					SELECT processinstance_ ,name_,stringvalue_ 
					FROM FAB_10_ECAP_DM.JBPM_VARIABLEINSTANCE
					WHERE name_ IN ( 'eCAP_Area' ,'Level' ,'Area' ,'Step' ,'Tool' ,'DTSString' )  
				) AS ecap_variables 
		group by processinstance_
		) as piv
		where Level IS NOT NULL
	)  
	iv  
	ON i.id_ = iv.processinstance_  
	LEFT OUTER JOIN  
	(  
		SELECT l.token_ ,	l.node_ ,l.leave_ ,	row_number() OVER ( PARTITION BY l.token_ ORDER BY l.leave_ DESC ) AS rn  
		FROM FAB_10_ECAP_DM.JBPM_LOG l  
		WHERE l.leave_ >= (CURRENT_DATE() - (8 * 7))  
	)  
	l  
	ON i.roottoken_ = l.token_  
	AND l.rn = 1
	INNER JOIN FAB_10_ECAP_DM.T_ECAP_INSTANCES tei  
	ON i.id_ = tei.instance_id -- pulls instance status information as well as includes mapping  
	-- between parent and sub-eCAP instances

	WHERE 
    --tei.status = 'C' AND
	d.name_ LIKE 'F10%_Observation%' -- for GOLDEN naming AND d.NAME_ LIKE 'F2__\_%Observation' ESCAPE '\' -- Look for eCAP templates which match golden naming convention 
        AND COALESCE(s.exval_00,'UNKNOWN') <> 'ECAP' -- filters out PRODUCT_FAMILY = ECAP (eCAP testing)
) A


/*

THIS SECTION OF CODE IS TO NARROW DOWN THE NUMBER OF ROWS WE ARE USING FROM THE spaceDWH.[olap].[D_mfg_year_month_ww] table
MWORKMAN. 2017.01.11

*/

INNER JOIN
(
	SELECT max_lastNodeUpdate, min_lastNodeUpdate, WW.*
	FROM 
	(
		SELECT MAX(lastNodeUpdate) AS max_lastNodeUpdate, MIN(lastNodeUpdate) AS min_lastNodeUpdate
		FROM
		(
			SELECT  
				area 
				--, wwUpdate.mfg_year_ww_num AS WW -- returns micron WW for the update time
				, template_name 
				, ch_id 
				, ch_name 
				, seq_num 
				, design_id 
				, lot_id 
				, parameter_name 
				, step_name 
				, process_tool_name 
				, process_tool_type 
				, process_position 
				, process_step_name 
				, process_recipe
				, instance_type
				, eCAP_Area 
				, Impacted_Level 
				, Impacting_Area 
				, Impacting_Step 
				, Impacted_Tool
				, DTSString 
				, collectionType 
				, chartType 
				, instance_id 
				, lastNodeUpdate 
				, observationStartTime  
			FROM
			(
			SELECT  
				CASE  
					WHEN iv.Level IS NOT NULL THEN SUBSTRING(iv.Area,4,length(iv.Area)-POSITION(' ' IN iv.Area))  
					ELSE s.cf_value_11  
				END AS area 
				--, wwUpdate.mfg_year_ww_num AS WW -- returns micron WW for the update time
				, d.name_ AS template_name 
				, s.ch_id AS ch_id 
				, s.exval_04 || '/' || s.parameter_name As ch_name
				, s.seq_num 
				, s.exval_01 AS design_id 
				, s.exval_12 AS lot_id 
				, s.parameter_name 
				, s.exval_04 AS step_name 
				, s.exval_07 AS process_tool_name 
				, s.exval_08 AS process_tool_type 
				, s.exval_11 AS process_position 
				, s.exval_06 AS process_step_name 
				, s.exval_10 AS process_recipe 
				, CASE  
					WHEN iv.Level IS NOT NULL THEN 'RDA'  
					ELSE 'SPACE'  
				END AS instance_type
				, iv.eCAP_Area 
				, iv.Level AS Impacted_Level 
				, iv.Area AS Impacting_Area 
				, iv.Step AS Impacting_Step 
				, iv.Tool AS Impacted_Tool
				, iv.DTSString 
				, s.cf_value_09 AS collectionType 
				, s.cf_value_10 AS chartType 
				, i.id_ AS instance_id 
				, COALESCE(i.end_,l.leave_) AS lastNodeUpdate 
				, i.start_ AS observationStartTime  
			FROM 
				FAB_10_ECAP_DM.JBPM_PROCESSINSTANCE i  
				INNER JOIN FAB_10_ECAP_DM.JBPM_PROCESSDEFINITION d  
				ON i.processdefinition_ = d.id_  
				INNER JOIN FAB_10_ECAP_DM.T_SPACE2ECAP s  
				ON i.id_ = s.instance_id -- to account for multiple wafers/parameters AND s.seq_num = 1
				LEFT JOIN FAB_10_ECAP_DM.JBPM_TOKEN t  
				ON i.id_ = t.processinstance_  
				LEFT JOIN FAB_10_ECAP_DM.JBPM_NODE n  
				ON t.node_ = n.id_  
				LEFT JOIN  
				(  
					select * from
					(
					select processinstance_,
					    max(case when name_ = 'eCAP_Area' then stringvalue_ end) eCAP_Area,
					    max(case when name_ = 'Level' then stringvalue_ end) "Level",
					    max(case when name_ = 'Area' then stringvalue_ end) Area,
						max(case when name_ = 'Step' then stringvalue_ end) Step,
						max(case when name_ = 'Tool' then stringvalue_ end) Tool,
						max(case when name_ = 'DTSString' then stringvalue_ end) DTSString
					from
					(  
								SELECT processinstance_ ,name_,stringvalue_ 
								FROM FAB_10_ECAP_DM.JBPM_VARIABLEINSTANCE
								WHERE name_ IN ( 'eCAP_Area' ,'Level' ,'Area' ,'Step' ,'Tool' ,'DTSString' )  
							) AS ecap_variables 
					group by processinstance_
					) as piv
					where Level IS NOT NULL
				)  
				iv  
				ON i.id_ = iv.processinstance_  
				LEFT OUTER JOIN  
				(  
					SELECT l.token_ ,	l.node_ ,l.leave_ ,	row_number() OVER ( PARTITION BY l.token_ ORDER BY l.leave_ DESC ) AS rn  
					FROM FAB_10_ECAP_DM.JBPM_LOG l  
					WHERE l.leave_ >= (CURRENT_DATE() - (8 * 7))  
				)  
				l  
				ON i.roottoken_ = l.token_  
				AND l.rn = 1  
				INNER JOIN FAB_10_ECAP_DM.T_ECAP_INSTANCES tei  
				ON i.id_ = tei.instance_id -- pulls instance status information as well as includes mapping  
				-- between parent and sub-eCAP instances

				WHERE 
                --tei.status = 'C' AND 
				d.name_ LIKE 'F10%_Observation%' -- for GOLDEN naming AND d.NAME_ LIKE 'F2__\_%Observation' ESCAPE '\' -- Look for eCAP templates which match golden naming convention 
				AND COALESCE(s.exval_00,'UNKNOWN') <> 'ECAP' -- filters out PRODUCT_FAMILY = ECAP (eCAP testing)
			) A
		) B
	) C

	INNER JOIN FAB_10_REF_DM.MFG_YEAR_MONTH_WW WW
	ON WW.mfg_ww_end_datetime >= C.min_lastNodeUpdate
	AND WW.mfg_ww_begin_datetime <= C.max_lastNodeUpdate
) WW


ON lastNodeUpdate >= WW.mfg_ww_begin_datetime  
AND lastNodeUpdate < WW.mfg_ww_end_datetime  

WHERE lastNodeUpdate >= (CURRENT_DATE() - (8 * 7));