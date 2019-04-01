CREATE VIEW GAYUS.F10N_eCAP_WS_View AS
SELECT 
  "isvhistory"."WS_GROUP_NAME" AS "Workstation",
  "isvhistory"."FRAME_EQUIP_ID"
FROM "GAYUS"."F10N_eCAP_COMBINED_View" "f10n_ecap_combined_view"
	RIGHT OUTER JOIN "FAB_10_OIM_DM"."ISVHISTORY" "isvhistory" ON (("f10n_ecap_combined_view"."process_tool_name" = "isvhistory"."FRAME_EQUIP_ID") AND ("f10n_ecap_combined_view"."area" = "isvhistory"."MFG_AREA_ID")) ;