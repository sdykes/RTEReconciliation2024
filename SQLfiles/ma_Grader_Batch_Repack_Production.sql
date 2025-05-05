SELECT 
	re.GraderBatchID, 
	CAST(NULL AS int) AS BlockID, 
	'Unknown' AS BlockCode, 
	'Unknown' AS BlockName, 
	sb.SubdivisionID, 
	sb.SubdivisionCode, 
	pd.PalletDetailID, 
	pd.ProductID, 
	re.SeasonID, 
	SUM(cam.Tubes) AS Tubes, 
	SUM(cam.NoOfUnits) AS NoOfUnits, 
	SUM(cam.RTEs) AS RTEs, 
	SUM(cam.Kgs) AS Kgs
FROM  dbo.ma_RepackT AS re 
INNER JOIN
   ma_Repack_Input_CartonT AS ca 
ON ca.RepackID = re.RepackID 
INNER JOIN
   (
   SELECT 
		ric.RepackInputCartonID, 
		CAST(CASE 
				WHEN ric.FromExportBinDetailID IS NULL THEN 1 
				ELSE 0 
			END AS numeric(10, 2)) AS NoOfUnits, 
		CASE 
			WHEN ric.FromExportBinDetailID IS NULL THEN 1 
			ELSE 0 
		END AS Tubes, 
		CAST(CASE 
				WHEN ric.FromExportBinDetailID IS NULL THEN pr.TubesPerCarton * pr.RTEConversion 
				ELSE (ebd.KGWeight / pr.PresizeAvgTubeWeight) * pr.RTEConversion 
			END AS numeric(10, 2)) AS RTEs, 
		CASE 
			WHEN ric.FromExportBinDetailID IS NULL THEN pr.NetFruitWeight 
			ELSE ebd.KGWeight 
		END AS Kgs
	FROM  dbo.ma_Repack_Input_CartonT AS ric 
	INNER JOIN
		ma_Pallet_DetailT AS pd 
	ON pd.PalletDetailID = ric.FromPalletDetailID 
	INNER JOIN
		(
		SELECT 
			pt.ProductID,
			pt.NetFruitWeight,
			pt.TubesPerCarton,
			pt.SampleFlag,
			tt.RTEConversion,
			tt.PresizeAvgTubeWeight
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
		) AS pr
	ON pr.ProductID = pd.ProductID
	LEFT OUTER JOIN
		ma_Export_Bin_DetailT AS ebd 
	ON ebd.ExportBinDetailID = ric.FromExportBinDetailID 
	LEFT OUTER JOIN
		(
		SELECT 
			ExportBinID, 
			SUM(KGWeight) AS TotalExportBinWeight
		FROM ma_Export_Bin_DetailT
		GROUP BY ExportBinID
		) AS eto 
	ON eto.ExportBinID = ebd.ExportBinID
	) AS cam
ON cam.RepackInputCartonID = ca.RepackInputCartonID 
INNER JOIN
   ma_Pallet_DetailT AS pd 
ON pd.PalletDetailID = ca.FromPalletDetailID 
INNER JOIN
   ma_Grader_BatchT AS gb 
ON gb.GraderBatchID = re.GraderBatchID 
INNER JOIN
   sw_SubdivisionT AS sb 
ON sb.SubdivisionID = gb.SubdivisionID
WHERE (gb.PresizeInputFlag = 0)
GROUP BY re.GraderBatchID, sb.SubdivisionID, sb.SubdivisionCode, pd.PalletDetailID, pd.ProductID, re.SeasonID







