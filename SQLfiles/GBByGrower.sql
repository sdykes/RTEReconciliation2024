SELECT 
	gb.GraderBatchID,
	Season,
	FarmCode,
	FarmName,
	GrowerName,
	ProductionSite,
	CASE
		WHEN
			StorageTypeID IN (6,7) THEN 'RA'
		ELSE 'CA'
	END AS StorageType,
	Maturity,
	PickNo,
	PackDate,
	PackingSite,
	InputKgs,
	WasteOtherKgs + ISNULL(rjk.JuiceKgs,0) + ISNULL(rsk.SampleKgs,0) AS RejectKgs
FROM ma_Grader_BatchT AS gb
INNER JOIN
	(
	SELECT 
		FarmID,
		FarmCode,
		FarmName,
		GrowerName
	FROM sw_FarmT AS ft
	INNER JOIN
		(
		SELECT 
			CompanyID,
			CompanyName AS GrowerName
		FROM sw_CompanyT
		) AS ct
	ON ct.CompanyID = ft.GrowerCompanyID
	) AS fm
ON fm.FarmID = gb.FarmID
INNER JOIN
	(
	SELECT
		SubdivisionID,
		SubdivisionCode AS ProductionSite
	FROM sw_SubdivisionT
	) AS st
ON st.SubdivisionID = gb.SubdivisionID
INNER JOIN
	(
	SELECT 
		CompanyID,
		CompanyName AS PackingSite
	FROM sw_CompanyT
	) AS ct2
ON ct2.CompanyID = gb.PackingCompanyID
INNER JOIN
	(
	SELECT
		MaturityID,
		MaturityCode AS Maturity
	FROM sw_MaturityT
	) AS mt
ON mt.MaturityID = gb.MaturityID
INNER JOIN
	(
	SELECT
		PickNoID,
		PickNoDesc AS PickNo
	FROM sw_Pick_NoT
	) AS pk
ON pk.PickNoID = gb.PickNoID
INNER JOIN
	(
	SELECT
		SeasonID,
		SeasonDesc AS Season
	FROM sw_SeasonT
	) AS se
ON se.SeasonID = gb.SeasonID
/* Calculating the JuiceKgs */
LEFT JOIN
    (
    SELECT
        PresizeOutputFromGraderBatchID AS GraderBatchID,
        SUM(TotalWeight) AS JuiceKgs
    FROM ma_Bin_DeliveryT
    WHERE PresizeProductID = 278 /*278 is the ProductID for juice bins*/
    GROUP BY PresizeOutputFromGraderBatchID
    ) AS rjk
ON gb.GraderBatchID = rjk.GraderBatchID
/* Calculating the sample KGs */
LEFT JOIN
    (
    SELECT 
        pd.GraderBatchID,
        SUM(pd.NoOfUnits*pt.NetFruitWeight) AS SampleKgs
    FROM ma_Pallet_DetailT AS pd
    INNER JOIN
        (
        SELECT
			ProductID,
		    NetFruitWeight
	    FROM sw_ProductT
	    WHERE SampleFlag = 1
	    ) AS pt
	ON pd.ProductID = pt.ProductID
	GROUP BY GraderBatchID
	) AS rsk
ON gb.GraderBatchID = rsk.GraderBatchID

