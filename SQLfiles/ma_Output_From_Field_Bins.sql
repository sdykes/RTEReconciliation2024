SELECT
	pa.DespatchID, 
	pd.GraderBatchID, 
	pd.PalletDetailID, 
	pd.ProductID, 
	fb.BlockID, 
	ISNULL(fb.BlockCode, 'Unknown') AS BlockCode, 
	ISNULL(fb.BlockName, 'Unknown') AS BlockName, 
	ca.PalletDamageID, 
	COUNT(DISTINCT ca.CartonNo) AS NoOfUnits, 
	COUNT(DISTINCT ca.CartonNo) * pr.TubesPerCarton * pr.RTEConversion AS RTEs, 
	COUNT(DISTINCT ca.CartonNo) * pr.NetFruitWeight AS Kgs, 
	pa.SeasonID, 
	sb.SubdivisionID, 
	sb.SubdivisionCode, 
	bi.MaturityID
FROM  ma_PalletT AS pa 
INNER JOIN
	ma_Pallet_DetailT AS pd 
ON pd.PalletID = pa.PalletID 
INNER JOIN
	ma_Grader_BatchT AS gb 
ON gb.GraderBatchID = pd.GraderBatchID 
INNER JOIN
	sw_SubdivisionT AS sb 
ON sb.SubdivisionID = gb.SubdivisionID 
INNER JOIN
	ma_CartonT AS ca 
ON ca.PalletDetailID = pd.PalletDetailID 
INNER JOIN
	(
	SELECT 
		pt.ProductID,
		pt.NetFruitWeight,
		pt.TubesPerCarton,
		pt.SampleFlag,
		tt.RTEConversion,
		pty.PoolByRule
	FROM sw_ProductT AS pt 
	INNER JOIN 
		(
		SELECT	
			TubeTypeID,
			RTEConversion,
			PresizeAvgTubeWeight
		FROM sw_Tube_TypeT
		) AS tt
	ON tt.TubeTypeID = pt.TubeTypeID
	INNER JOIN
		(
		SELECT 
			GradeID,
			PoolByRule
		FROM sw_GradeT AS gt
		INNER JOIN
			(
			SELECT 
				PoolTypeID,
				PoolByRule
			FROM sys_fi_Pool_TypeT
			) AS sfpt
		ON sfpt.PoolTypeID = gt.PoolTypeID
		) AS pty
	ON pty.GradeID = pt.GradeID
	) AS pr
ON pr.ProductID = pd.ProductID
LEFT JOIN
	ma_Bin_DeliveryT AS bi 
ON bi.BinDeliveryID = ca.BinDeliveryID 
LEFT JOIN
	sw_Farm_BlockT AS fb 
ON fb.BlockID = bi.BlockID
WHERE gb.PresizeInputFlag = 0 
AND pr.SampleFlag = 0
GROUP BY 
	pa.DespatchID, 
	pd.GraderBatchID, 
	pd.PalletDetailID, 
	pd.ProductID, 
	pr.PoolByRule, 
	pr.RTEConversion, 
	pr.TubesPerCarton, 
	pr.NetFruitWeight, 
	pa.SeasonID, 
	fb.BlockID, 
	fb.BlockCode, 
	fb.BlockName, 
	ca.PalletDamageID, 
	sb.SubdivisionID, 
	sb.SubdivisionCode, 
	bi.MaturityID
UNION ALL
SELECT 
	pa.DespatchID, 
	pd.GraderBatchID, 
	pd.PalletDetailID, 
	pd.ProductID, NULL AS BlockID/* do not carton/bin delivery as there will be many within an export bin.*/ , 
	'Unknown' AS BlockCode, 
	'Unknown' AS BlockName, 
	NULL AS PalletDamageID, 
	pd.NoOfUnits AS NoOfUnits, 
	SUM(CAST((ebd.KGWeight / pr.PresizeAvgTubeWeight) * pr.RTEConversion AS numeric(10, 2))) AS RTEs, 
	SUM(ebd.KGWeight) AS Kgs, 
	pa.SeasonID, 
	sb.SubdivisionID, 
	sb.SubdivisionCode, 
	gb.MaturityID
FROM  ma_PalletT AS pa 
INNER JOIN
      ma_Pallet_DetailT pd 
ON pd.PalletID = pa.PalletID 
INNER JOIN
      ma_Grader_BatchT gb 
ON gb.GraderBatchID = pd.GraderBatchID 
INNER JOIN
      sw_SubdivisionT sb 
ON sb.SubdivisionID = gb.SubdivisionID 
INNER JOIN
       ma_Export_Bin_DetailT ebd 
ON ebd.PalletDetailID = pd.PalletDetailID 
INNER JOIN
	(
	SELECT 
		pt.ProductID,
		pt.NetFruitWeight,
		pt.TubesPerCarton,
		pt.SampleFlag,
		tt.RTEConversion,
		tt.PresizeAvgTubeWeight,
		pty.PoolByRule
	FROM sw_ProductT AS pt 
	INNER JOIN 
		(
		SELECT	
			TubeTypeID,
			RTEConversion,
			PresizeAvgTubeWeight
		FROM sw_Tube_TypeT
		) AS tt
	ON tt.TubeTypeID = pt.TubeTypeID
	INNER JOIN
		(
		SELECT 
			GradeID,
			PoolByRule
		FROM sw_GradeT AS gt
		INNER JOIN
			(
			SELECT 
				PoolTypeID,
				PoolByRule
			FROM sys_fi_Pool_TypeT
			) AS sfpt
		ON sfpt.PoolTypeID = gt.PoolTypeID
		) AS pty
	ON pty.GradeID = pt.GradeID
	) AS pr
ON pr.ProductID = pd.ProductID
WHERE gb.PresizeInputFlag = 0 AND pr.SampleFlag = 0
GROUP BY 
	pa.DespatchID, 
	pd.GraderBatchID, 
	pd.PalletDetailID, 
	pd.ProductID, 
	pd.NoOfUnits, 
	pa.SeasonID, 
	sb.SubdivisionID, 
	sb.SubdivisionCode, 
	gb.MaturityID


	 
