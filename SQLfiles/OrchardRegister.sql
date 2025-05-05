SELECT 
	FarmName,
	FarmCode AS PIN,
	ManagerName,
	SubdivisionCode AS Subdivision,
	BlockCode AS ManagementArea,
	CompanyName AS Owner
FROM sw_Farm_BlockT AS fbt
INNER JOIN
	sw_FarmT AS ft
ON ft.FarmID = fbt.FarmID
INNER JOIN
	sw_SubdivisionT AS st
ON st.SubdivisionID = fbt.SubdivisionID
INNER JOIN
	sw_CompanyT AS ct
ON ct.CompanyID = GrowerCompanyID 

