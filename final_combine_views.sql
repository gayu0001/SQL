WITH  F10N_eCAP_DAM_View AS(
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
	INNER JOIN  FAB_10_SPC_DM.T_CHANNEL_DEF scd
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

WHERE lastNodeUpdate >= (CURRENT_DATE() - (8 * 7)))
,RootCauseInstances --WITH: create temp table to be used in later joins, referencing...
AS (
  SELECT tei.parent_instance_id AS parentInstance
    , tei.instance_id AS subEcapInstance
    , CASE 
      WHEN COUNT(op.parameter_value) > 0
        THEN 1.00
      ELSE 0.00
      END as root_cause_found -- If the # of returns parameters = true is > 0, return 1 for root cause found; otherwise, return 0 for no root cause.
  FROM FAB_10_ECAP_DM.T_ECAP_INSTANCES tei
  LEFT JOIN FAB_10_ECAP_DM.JBPM_PROCESSINSTANCE i
    ON tei.instance_id = i.id_
  LEFT JOIN FAB_10_ECAP_DM.JBPM_PROCESSDEFINITION d
    ON i.processdefinition_ = d.id_
  LEFT JOIN FAB_10_ECAP_DM.T_ECAP_INSTANCE_OUTPUTPARAMS op
    ON tei.instance_id = op.instance_id
  WHERE tei.status = 'C' -- Completed eCAP instances
    AND d.name_ LIKE '%Investigation_Modules%' -- Looks for output parameters on the Investigation_Modules eCAP
    AND op.parameter_name LIKE 'RC%' -- Looks for parameters which start with RC
    --AND op.parameter_value = 'true' -- and that have a value equal to true.
  GROUP BY tei.parent_instance_id
    , tei.instance_id
  )
  , RootCauseDetails
AS (
  SELECT *
  FROM (
    select instance_id,
		    max(case when parameter_name = 'RC' then parameter_value end) RC,
		    max(case when parameter_name = 'RC_Charting' then parameter_value end) RC_Charting,
		    max(case when parameter_name = 'RC_Equip' then parameter_value end) RC_Equip,
			max(case when parameter_name = 'RC_Incoming' then parameter_value end) RC_Incoming,
			max(case when parameter_name = 'RC_Metro' then parameter_value end) RC_Metro,
			max(case when parameter_name = 'RC_Process' then parameter_value end) RC_Process,
			max(case when parameter_name = 'RC_None' then parameter_value end) RC_None,
			max(case when parameter_name = 'RC_SWR' then parameter_value end) RC_SWR,
			max(case when parameter_name = 'RQDR' then parameter_value end) RQDR,
			max(case when parameter_name = 'DQDR' then parameter_value end) DQDR,
			max(case when parameter_name = 'CQDR' then parameter_value end) CQDR,
			max(case when parameter_name = 'SQDR' then parameter_value end) SQDR
		from
		(  
					SELECT instance_id
				      , parameter_name
				      , parameter_value
				    FROM FAB_10_ECAP_DM.T_ECAP_INSTANCE_OUTPUTPARAMS
				    WHERE parameter_name IN (
				        'RC'
				        , 'RC_Charting'
				        , 'RC_Equip'
				        , 'RC_Incoming'
				        , 'RC_Metro'
				        , 'RC_Process'
				        , 'RC_None'
				        , 'RC_SWR'
				        , 'RQDR'
				        , 'DQDR'
				        , 'CQDR'
				        , 'SQDR'
				        )
				    ) AS ecap_variables
		group by instance_id
        ) AS piv
  )
  , VisitBypassDetails
AS (
  SELECT *
  FROM (
    select instance_id,
		    max(case when parameter_name = 'Charting_Visit' then parameter_value end) Charting_Visit,
		    max(case when parameter_name = 'Equip_Visit' then parameter_value end) Equip_Visit,
		    max(case when parameter_name = 'Incoming_Visit' then parameter_value end) Incoming_Visit,
			max(case when parameter_name = 'Metro_Visit' then parameter_value end) Metro_Visit,
			max(case when parameter_name = 'Process_Visit' then parameter_value end) Process_Visit,
			max(case when parameter_name = 'SWR_Visit' then parameter_value end) SWR_Visit,
			max(case when parameter_name = 'Bypass_Metric' then parameter_value end) Bypass_Metric,
			max(case when parameter_name = 'Bypass_Metro_Metric' then parameter_value end) Bypass_Metro_Metric
		from 
		(  
				SELECT instance_id
			      , parameter_name
			      , parameter_value
			    FROM FAB_10_ECAP_DM.T_ECAP_INSTANCE_OUTPUTPARAMS
			    WHERE parameter_name IN (
			                   'Charting_Visit',
			                   'Equip_Visit',
			                   'Incoming_Visit',
			                   'Metro_Visit',
			                   'Process_Visit',
			                   'SWR_Visit',
			                   'Bypass_Metric',
			        			'Bypass_Metro_Metric'
			        )
    ) AS ecap_variables
		group by instance_id
        ) AS piv
  ), InvestigationEnd
AS (
          SELECT jpi.id_ AS processInstance
                   ,l.leave_ AS investigationEndTime
          FROM FAB_10_ECAP_DM.JBPM_NODE n
          INNER JOIN FAB_10_ECAP_DM.JBPM_PROCESSDEFINITION d ON n.processdefinition_ = d.id_
          INNER JOIN FAB_10_ECAP_DM.JBPM_PROCESSINSTANCE jpi ON jpi.processdefinition_ = d.id_
          INNER JOIN FAB_10_ECAP_DM.JBPM_LOG l ON n.id_ = l.node_
                   AND l.token_ = jpi.roottoken_
          WHERE n.name_ = 'Investigation_Start' -- Look for nodes with the the name 'Investigation_Start'
          )
  , ObservationNodeList
AS (
  SELECT CASE 
      WHEN iv.eCAP_Area IS NOT NULL
        THEN LTRIM(SUBSTRING(iv.eCAP_Area, POSITION(' ' IN iv.eCAP_Area), LENGTH(iv.eCAP_Area) - POSITION(' ' IN iv.eCAP_Area) + 1))
      ELSE "cd".cf_value_11
      END AS area
    , d.name_ AS ecapTemplateName
    , s.ch_id AS CH_ID
    , CASE 
      WHEN iv.LEVEL IS NOT NULL
        THEN 'RDA'
      ELSE 'SPACE'
      END AS instance_type
	, CASE
			WHEN POSITION('LOTDISPO' IN d.name_) > 01 -- if the eCAP template contains LOTDISPO, make the instanceType as DISPO
				THEN 'DISPO'
			WHEN d.name_ LIKE '%_FD_%' --AND iv.Level <> 'H' -- if the parameter contains FD, and the Level is Low, make the instanceType as FD
				THEN 'FD'
--			WHEN d.name_ LIKE '%_FD_%' AND iv.Level = 'H' -- if the parameter contains FD, and the Level is High, then consider as FD Chronic OOC
--			THEN 'FD_Chronic'
			WHEN d.name_ LIKE '%_QT_%' -- If the template name contian _QT_ then consider as Qtime instance Type
				THEN 'QTIME'
			ELSE 
				CASE 
					WHEN iv.LEVEL = 'H' 
						THEN 'SPACE_Chronic'
					ELSE 'SPACE'
				END
			END AS instanceType
    , COALESCE(iv.eCAP_Area, "cd".cf_value_11) AS eCAP_Area
    , COALESCE(iv.LEVEL, 'NA') AS Impacted_Level
    , COALESCE(iv.Area, 'NA') AS Impacting_Area
    , COALESCE(iv.Step, 'NA') AS Impacting_Step
    , COALESCE(iv.Tool, 'NA') AS Impacted_Tool
    , COALESCE(iv.DTSString, 'NA') AS DTSString
          , CASE WHEN COALESCE(iv.Remeasure, 'false') = 'true' THEN 1.00
      ELSE 0.00
      END Remeasure
    , s.parameter_name
    , s.exval_07 AS process_tool_name
    , s.exval_11 AS process_position
    , s.exval_05 AS metro_tool_name
    , s.exval_12 AS lot_id
    , s.exval_01 AS design_id
    , s.daval_03 AS wafer_id
    , s.exval_14 AS special_flags
    , s.exval_06 AS process_step_name
    , s.exval_04 AS metro_step_name
    , s.exval_08 AS process_tool_type
	, s.cf_value_10 AS chart_type
    , COALESCE(vi.stringvalue_, 'Not Assigned') AS operator
    , OREPLACE(OREPLACE(s.violations, '&nbsp;', ' '), '<br>', '; ') AS violations
    , tei.status
    , COALESCE(rci.root_cause_found, 0) AS root_cause
    , i.id_ AS observationProcessInstance
    , COALESCE(rci.subEcapInstance, 0) AS subEcapInstance
    , i.end_ AS observationEndTime
	, i.start_ AS observationStartTime
	, wwStart.mfg_year_no || trim(wwStart.mfg_ww_no(format '99')) AS observationStartTime_WW
    , wwStart.mfg_ww_seq_no AS observationStartTime_WW_seqno
	,(CAST((CAST(i.end_ AS DATE)- CAST(i.start_ AS DATE)) AS DECIMAL(18,6)) * 60*24)
  + ((EXTRACT(  HOUR FROM i.end_) - EXTRACT(  HOUR FROM i.start_))* 60)
  + ((EXTRACT(MINUTE FROM i.end_) - EXTRACT(MINUTE FROM i.start_))  )
  + ((EXTRACT(SECOND FROM i.end_) - EXTRACT(SECOND FROM i.start_))/60)
	AS minToClose
    --, i.end_ - i.start_ AS minToClose
    , rcd.RC
    , rcd.RC_Charting
    , rcd.RC_Equip
    , rcd.RC_Incoming
    , rcd.RC_Metro
    , rcd.RC_Process
          , rcd.RC_None
          , rcd.RC_SWR
    , CASE 
      WHEN rcd.DQDR = 'true'
        THEN 1.00
      ELSE 0.00
      END AS DQDR
    , CASE 
      WHEN rcd.RQDR = 'true'
        THEN 1.00
      ELSE 0.00
      END AS RQDR
    , CASE 
      WHEN rcd.CQDR = 'true'
        THEN 1.00
      ELSE 0.00
      END AS CQDR
    , CASE 
      WHEN rcd.SQDR = 'true'
        THEN 1.00
      ELSE 0.00
      END AS SQDR
          , CASE
            WHEN vd.Bypass_Metric = 'true'
              THEN 1.00
            ELSE 0.00
            END AS Bypass_Metric
          , CASE
            WHEN vd.Bypass_Metro_Metric = 'true'
              THEN 1.00
            ELSE 0.00
            END AS Bypass_Metro_Metric
    , COALESCE(vd.Charting_Visit,'Unknown') AS Charting_Visit
    , COALESCE(vd.Equip_Visit,'Unknown') AS Equip_Visit
    , COALESCE(vd.Incoming_Visit,'Unknown') AS Incoming_Visit
    , COALESCE(vd.Metro_Visit,'Unknown') AS Metro_Visit
    , COALESCE(vd.Process_Visit,'Unknown') AS Process_Visit
    , COALESCE(vd.SWR_Visit,'Unknown') AS SWR_Visit
  FROM FAB_10_ECAP_DM.JBPM_PROCESSINSTANCE i
  LEFT JOIN FAB_10_ECAP_DM.JBPM_PROCESSDEFINITION d
    ON i.processdefinition_ = d.id_
  LEFT JOIN FAB_10_ECAP_DM.T_SPACE2ECAP s
    ON i.id_ = s.instance_id
      AND s.seq_num = 1 --include s.seq_num to prevent join on multiple CH/CKC's (due to eCAP MRG)
  LEFT JOIN FAB_10_ECAP_DM.T_ECAP_INSTANCES tei
    ON i.id_ = tei.instance_id -- pulls instance status information as well as includes mapping between parent and sub-eCAP instances
  LEFT JOIN FAB_10_ECAP_DM.JBPM_TOKEN t
    ON i.id_ = t.processinstance_
  LEFT JOIN FAB_10_ECAP_DM.JBPM_NODE n
    ON t.node_ = n.id_
  LEFT JOIN FAB_10_ECAP_DM.JBPM_VARIABLEINSTANCE vi
    ON i.id_ = vi.processinstance_
      AND vi.name_ = 'operator'
  LEFT JOIN (
    SELECT *
    FROM (
      select processinstance_,
		    max(case when name_ = 'eCAP_Area' then stringvalue_ end) eCAP_Area,
		    max(case when name_ = 'Level' then stringvalue_ end) "Level",
		    max(case when name_ = 'Area' then stringvalue_ end) Area,
			max(case when name_ = 'Step' then stringvalue_ end) Step,
			max(case when name_ = 'Tool' then stringvalue_ end) Tool,
			max(case when name_ = 'DTSString' then stringvalue_ end) DTSString,
			max(case when name_ = 'Remeasure' then stringvalue_ end) Remeasure
		from 
		(  
				SELECT processinstance_
		        , name_
		        , stringvalue_
		      FROM FAB_10_ECAP_DM.JBPM_VARIABLEINSTANCE
		      WHERE name_ IN (
		          'eCAP_Area'
		          , 'Level'
		          , 'Area'
		          , 'Step'
		          , 'Tool'
		          , 'DTSString'
		                     , 'Remeasure'
		          )
		      ) AS ecap_variables
		group by processinstance_
          ) AS piv
    ) iv
    ON i.id_ = iv.processinstance_
  LEFT JOIN RootCauseInstances rci
    ON i.id_ = rci.parentInstance
  LEFT JOIN RootCauseDetails rcd
    ON rci.subEcapInstance = rcd.instance_id
  LEFT JOIN VisitBypassDetails vd
    ON rci.subEcapInstance = vd.instance_id
  LEFT JOIN InvestigationEnd ie
    ON i.id_ = ie.processInstance
  INNER JOIN FAB_10_SPC_DM.T_CHANNEL_DEF "cd"
    ON s.ch_id = "cd".ch_id -- join on the channel ID to pull in channel properties
  INNER JOIN FAB_10_REF_DM.mfg_year_month_ww wwStart
    ON i.start_ >= wwStart.mfg_ww_begin_datetime
      AND i.start_ < wwStart.mfg_ww_end_datetime
  WHERE d.name_ LIKE 'F10%_Observation%' -- Look for eCAP templates which match golden naming convention
    AND POSITION('LOTDISPO' IN d.name_) = 0 -- excludes all LOTDISPO templates since they should not be included in historic review
    AND tei.status = 'C' -- !!! Look for completed eCAP instances !!!
          AND COALESCE(s.exval_00,'UNKNOWN') <> 'ECAP' -- excludes eCAP Buddy instances
    --AND i.end_ > (GETDATE() - 7) -- COMMENT OUT. Not required due to below date query by WW !!! Pulls all instances which occured in the past 7 days
  )

--START HERE
  SELECT	
	COALESCE(h.area,d.area) AS area, --selects first non-null value from given inputs; if both null, null value
	COALESCE(h.ecap_area,d.eCAP_Area) AS eCAP_Area,
	COALESCE(h.ecap_template_name,d.template_name) AS template_name,
	COALESCE(h.WW,d.WW) AS WW,
	COALESCE(h.ch_id,d.ch_id) AS ch_id,
	d.ch_name,
	COALESCE(h.design_id,d.design_id) AS design_id,
	COALESCE(h.lot_id,d.lot_id) AS lot_id,
	COALESCE(h.parameter_name,d.parameter_name) AS parameter_name,
	d.step_name,
	COALESCE(h.process_tool_name,d.process_tool_name) AS process_tool_name,
	COALESCE(h.process_tool_type,d.process_tool_type) AS process_tool_type,
	COALESCE(h.process_position,d.process_position) AS process_position,
	COALESCE(h.process_step_name,d.process_step_name) AS process_step_name,
	d.process_recipe,
	COALESCE(h.instance_id,d.instance_id) AS instance_id,
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
	h.remeASure,
	h.minToClose,
	Tool2.Workstation

FROM 
	(SELECT 
		 
	 	F10N_eCAP_Hist_View.area, F10N_eCAP_Hist_View.ww AS WW, F10N_eCAP_Hist_View.ecap_template_name, F10N_eCAP_Hist_View.observation_end_time,  F10N_eCAP_Hist_View.instance_id, F10N_eCAP_Hist_View.ch_id, F10N_eCAP_Hist_View."operator", F10N_eCAP_Hist_View.PARAMETER_NAME AS parameter_name, 
	 	F10N_eCAP_Hist_View.process_tool_name, F10N_eCAP_Hist_View.process_position, F10N_eCAP_Hist_View.metro_tool_name, F10N_eCAP_Hist_View.lot_id, F10N_eCAP_Hist_View.design_id, F10N_eCAP_Hist_View.wafer_id, F10N_eCAP_Hist_View.special_flags, F10N_eCAP_Hist_View.process_step_name, F10N_eCAP_Hist_View.metro_step_name, 
	 	F10N_eCAP_Hist_View.process_tool_type, F10N_eCAP_Hist_View.chart_type, F10N_eCAP_Hist_View.violations, F10N_eCAP_Hist_View.root_cause, F10N_eCAP_Hist_View.rc, F10N_eCAP_Hist_View.rc_charting, F10N_eCAP_Hist_View.rc_equip, F10N_eCAP_Hist_View.rc_incoming, F10N_eCAP_Hist_View.rc_metro, F10N_eCAP_Hist_View.rc_process, F10N_eCAP_Hist_View.rc_none, 
	 	F10N_eCAP_Hist_View.rc_swr, F10N_eCAP_Hist_View.ecap_area, F10N_eCAP_Hist_View.impacted_level, F10N_eCAP_Hist_View.impacting_area, F10N_eCAP_Hist_View.impacting_step, F10N_eCAP_Hist_View.impacted_tool, F10N_eCAP_Hist_View.dts_string, F10N_eCAP_Hist_View.remeASure, F10N_eCAP_Hist_View.minToClose 
	 
	FROM 
	(SELECT onl.area
	  , onl.observationStartTime_WW AS ww
	  , onl.ecapTemplateName AS ecap_template_name
	  , onl.observationEndTime AS observation_end_time
	  , onl.observationProcessInstance AS instance_id
	  , onl.subEcapInstance AS investigation_ecap_instance
	  , onl.instance_type
	  , onl.instanceType
	  , onl.CH_ID AS ch_id
	  , onl.operator
	  , onl.parameter_name
	  , onl.process_tool_name
	  , onl.process_position
	  , onl.metro_tool_name
	  , onl.lot_id
	  , onl.design_id
	  , onl.wafer_id
	  , onl.special_flags
	  , onl.process_step_name
	  , onl.metro_step_name
	  , onl.process_tool_type
	  , onl.chart_type
	  , onl.violations
	  , onl.root_cause
	  , COALESCE(onl.RC, 'No Root Cause Found') AS rc
	  , CASE 
	    WHEN onl.RC_Charting = 'true'
	      THEN 1.00
	    ELSE 0.00
	    END AS rc_charting
	  , CASE 
	    WHEN onl.RC_Equip = 'true'
	      THEN 1.00
	    ELSE 0.00
	    END AS rc_equip
	  , CASE 
	    WHEN onl.RC_Incoming = 'true'
	      THEN 1.00
	    ELSE 0.00
	    END AS rc_incoming
	  , CASE 
	    WHEN onl.RC_Metro = 'true'
	      THEN 1.00
	    ELSE 0.00
	    END AS rc_metro
	  , CASE 
	    WHEN onl.RC_Process = 'true'
	      THEN 1.00
	    ELSE 0.00
	    END AS rc_process
	  , CASE 
	    WHEN onl.RC_None = 'true'
	      THEN 1.00
	    ELSE 0.00
	    END AS rc_none
	  , CASE 
	    WHEN onl.RC_SWR = 'true'
	      THEN 1.00
	    ELSE 0.00
	    END AS rc_swr
	  , onl.DQDR AS dqdr
	  , onl.RQDR AS rqdr
	  , onl.CQDR AS cqdr
	  , onl.SQDR AS sqdr
	  , onl.Bypass_Metric AS bypass_metric
	  , onl.Bypass_Metro_Metric AS bypass_metro_metric
	  , CASE
	    WHEN onl.Charting_Visit ='true'
	            THEN 1.00
	          ELSE 0.00
	          END AS charting_visit
	  , CASE
	    WHEN onl.Equip_Visit ='true'
	            THEN 1.00
	          ELSE 0.00
	          END AS equip_visit
	  , CASE
	    WHEN onl.Incoming_Visit ='true'
	            THEN 1.00
	          ELSE 0.00
	          END AS incoming_visit
	  , CASE
	    WHEN onl.Metro_Visit ='true'
	            THEN 1.00
	          ELSE 0.00
	          END AS metro_visit
	  , CASE
	    WHEN onl.Process_Visit ='true'
	            THEN 1.00
	          ELSE 0.00
	          END AS process_visit
	  , CASE
	    WHEN onl.SWR_Visit ='true'
	            THEN 1.00
	          ELSE 0.00
	          END AS swr_visit
	  , onl.eCAP_Area AS ecap_area
	  , onl.Impacted_Level AS impacted_level
	  , onl.Impacting_Area AS impacting_area
	  , onl.Impacting_Step AS impacting_step
	  , onl.Impacted_Tool AS impacted_tool
	  , onl.DTSString AS dts_string
	  , onl.Remeasure AS remeasure
	  , COALESCE(onl.minToClose,-1) AS minToClose
	FROM ObservationNodeList onl) as F10N_eCAP_Hist_View
	WHERE ecap_template_name LIKE '%DF%') AS h
	
	FULL JOIN 
	
	(SELECT 
 
		"area", "eCAP_Area", "WW", "template_name", "ch_id", "ch_name", "design_id", "lot_id", "PARAMETER_NAME" AS "parameter_name", "step_name", 
		"process_tool_name", "process_tool_type", "process_position", "process_step_name", "process_recipe", "instance_id", "STATUS" AS "status", 
		"observationStartTime"
		
	FROM F10N_eCAP_DAM_View
	WHERE "template_name" LIKE '%DF%' AND "status" = 'P' OR "template_name" LIKE '%DF%' AND "status" = 'C' OR "template_name" LIKE '%DF%' AND "status" = 'A'
	/*filter out status=NA*/) AS d 
	ON (h.ecap_template_name=d.template_name AND h.area=d.area AND h.WW=d.WW AND h.ch_id=d.ch_id AND h.parameter_name=d.parameter_name 
		AND h.process_tool_name=d.process_tool_name AND h.process_position=d.process_position AND h.lot_id=d.lot_id AND h.design_id=d.design_id 
		AND h.process_step_name=d.process_step_name AND h.process_tool_type=d.process_tool_type AND h.instance_id=d.instance_id AND h.ecap_area=d.eCAP_Area)
	
	LEFT JOIN
		
	(SELECT 
 	Tool.equip_id, 
 	Tool.WS_name AS Workstation
 	FROM
 		(SELECT 

		RTRIM(AREA.mfg_area_id) AS mfg_area_id,
		RTRIM(EQUIP.equip_type_id) AS equip_type_id,
		RTRIM(EQUIP.equip_id) AS equip_id,
		RTRIM(WS.WS_name) AS WS_name,
		(CASE ESAREA.semi_state_id WHEN 'UNSCHEDULED_DOWNTIME' THEN 'UNSCHED_DOWN' WHEN 'SCHEDULED_DOWNTIME' THEN 'SCHED_DOWN' ELSE RTRIM(ESAREA.semi_state_id) END) AS semi_state_id,
		RTRIM(ESAREA.equip_state_id) AS equip_state_id,
		(CAST((CAST((CURRENT_TIMESTAMP+INTERVAL '8' HOUR) AS DATE)- CAST(EQSTAT.current_equip_state_datetime AS DATE)) AS DECIMAL(18,6)) *24)
  + (CAST((EXTRACT(  HOUR FROM (CURRENT_TIMESTAMP+INTERVAL '8' HOUR)) - EXTRACT(  HOUR FROM EQSTAT.current_equip_state_datetime)) AS DECIMAL(18,6)))
  + (CAST((EXTRACT(MINUTE FROM CURRENT_TIMESTAMP) - EXTRACT(MINUTE FROM EQSTAT.current_equip_state_datetime)) AS DECIMAL(18,6))/60  )
  + (CAST((EXTRACT(SECOND FROM CURRENT_TIMESTAMP) - EXTRACT(SECOND FROM EQSTAT.current_equip_state_datetime)) AS DECIMAL(18,6))/3600)
	AS Time_Difference_in_Hours_from_CurrentTime,
		(CAST((SUBSTR((TO_CHAR(EQSTAT.current_equip_state_datetime)),1,10)) AS DATE) - DATE '1899-12-30') AS Time_Difference_in_Days_from_30121899,
		(CAST((SUBSTR((TO_CHAR(EQSTAT.current_equip_state_datetime)),12,8)) AS TIME(0))) AS Time_of_Eqp_St_Datetime,
		RTRIM(EQUIP.equip_desc) AS equip_desc,
		RTRIM(EQUIP.physical_location) AS physical_location,
		(CASE WHEN SAP.micron_username IS NULL THEN 
			CASE EQSTAT.current_equip_state_emp_no WHEN 0 THEN '* AUTO TOOL *' ELSE CAST(EQSTAT.current_equip_state_emp_no AS CHAR) END
		WHEN SAP.micron_username='' THEN CAST(EQSTAT.current_equip_state_emp_no AS CHAR) 
		ELSE RTRIM(SAP.micron_username) END) AS micron_username,
		RTRIM(EQSTAT.current_equip_state_by_system) AS current_equip_state_by_system,
		NOTE.event_note_text AS event_note_text
		
		FROM
		
		FAB_10_ET_DM.EQUIPMENT EQUIP --(equip_tracking_DSS..equipment)
		INNER JOIN FAB_10_REF_DM.FP_EQUIP FPE --(reference..FP_equip)
		ON EQUIP.equip_OID = FPE.equip_OID
		LEFT OUTER JOIN FAB_10_REF_DM.FP_WS WS --(reference..FP_WS)
		ON FPE.WS_OID = WS.WS_OID
		INNER JOIN FAB_10_ET_DM.CURRENT_EQUIPMENT_STATE EQSTAT --(equip_tracking_DSS..current_equipment_state)
		ON EQUIP.equip_OID = EQSTAT.equip_OID
		INNER JOIN FAB_10_ET_DM.EQUIPMENT_STATE_FOR_AREA ESAREA --(equip_tracking_DSS..equipment_state_for_area)
		ON EQSTAT.equip_state_OID = ESAREA.equip_state_OID
		INNER JOIN FAB_10_REF_DM.MFG_AREA AREA --(reference..mfg_area)
		ON EQUIP.mfg_area_OID = AREA.mfg_area_OID
		LEFT OUTER JOIN FAB_10_REF_DM.SAP_WORKER SAP --(reference..SAP_worker)
		ON EQSTAT.current_equip_state_emp_no = SAP.worker_no
		LEFT OUTER JOIN FAB_10_ET_DM.EVENT_NOTE NOTE --(equip_tracking_DSS..event_note)
		ON EQSTAT.production_state_change_OID = NOTE.production_state_change_OID
		
		WHERE
		EQUIP.mfg_facility_OID = '8E3074D7400A9854'XBV AND
		EQUIP.equip_status = 'ACTIVE' AND
		EQUIP.equip_type_id NOT IN ('SCRUBBER','PUMP','CHILLER','LSC','TAP TEST','TRAINING','LOADPORT','LOADPORTNNC') 
		AND EQUIP.equip_type_id NOT LIKE 'FOUP%' AND EQUIP.equip_type_id NOT LIKE 'Z%'  
		AND AREA.mfg_area_id LIKE '%DIFFUSION%') AS Tool
	
		WHERE SUBSTR(Tool.equip_id,9,2)='00') AS Tool2
		
	ON Tool2.equip_id = COALESCE(h.process_tool_name,d.process_tool_name)
	WHERE Tool2.Workstation IS NOT NULL; 