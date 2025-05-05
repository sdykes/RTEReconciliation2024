SELECT
	gb.GraderBatchID, 
	gb.GraderBatchNo, 
	fa.FarmID, 
	fa.FarmName AS Orchard, 
	fa.FarmCode AS RPIN, 
	sb.SubdivisionID, 
	sb.SubdivisionCode AS [Production site], 
	gb.PackDate, 
	ct.CompanyName AS [Packing site],
	gb.ClosedDateTime, 
	st.StorageTypeCode, 
	st.StorageTypeDesc, 
	bu.BinKgs, 
	gb.InputKgs, 
	gb.[WasteOtherKgs] + ISNULL(jbo.JuiceWeight, 0) + ISNULL(sam.SampleWeight, 0) AS RejectKgs, 
	bu.NoOfFieldBins AS FieldBinsTipped, 
	bp.NoOfPresizeBins AS PresizeBins, 
    CASE
		WHEN 
			gb.PresizeInputFlag = 1 THEN gb.InputKgs 
		ELSE 0 
	END AS PresizeKgsTipped, 
	CASE 
		WHEN 
			bu.NoOfFieldBins = 0 THEN 0 
		ELSE gb.InputKgs / bu.NoOfFieldBins 
	END AS AvgFieldBinWeight, 
	pbo.PresizeWeight AS PresizeWeightOutput, 
	gb.Comment, 
	sn.SeasonDesc AS Season, 
	gb.MaturityID, 
	bm.MaturityCode, 
	gb.HarvestDate, 
	sh.ShiftCode, 
	CASE 
		WHEN 
			gb.ClosedDateTime IS NOT NULL THEN 'Closed' 
			ELSE 'Open' 
		END AS [BatchStatus], 
	gb.PickNoID
FROM  ma_Grader_BatchT AS gb 
LEFT JOIN
      sw_FarmT AS fa 
ON fa.FarmID = gb.FarmID 
LEFT JOIN
	  sw_SubdivisionT sb 
ON sb.SubdivisionID = gb.SubdivisionID 
LEFT JOIN
      sw_MaturityT ma 
ON ma.MaturityID = gb.MaturityID 
LEFT JOIN
      ma_ShiftT sh 
ON sh.ShiftID = gb.ShiftID 
LEFT JOIN
      sw_Storage_TypeT st 
ON st.StorageTypeID = gb.StorageTypeID 
LEFT JOIN
	sw_CompanyT AS ct
ON ct.CompanyID = gb.PackingCompanyID
INNER JOIN
	sw_SeasonT AS sn
ON sn.SeasonID = gb.SeasonID
/* Get Max Maturity Code from input Bins*/ 
LEFT JOIN
	(
	SELECT 
		bu.GraderBatchID, 
		MAX(ma.MaturityCode) AS MaturityCode
    FROM ma_Bin_DeliveryT bi 
	INNER JOIN
		ma_Bin_UsageT AS bu 
	ON bu.BinDeliveryID = bi.BinDeliveryID 
	LEFT JOIN
		sw_MaturityT ma 
	ON ma.MaturityID = bi.MaturityID
	GROUP BY bu.GraderBatchID
	) AS bm 
ON bm.GraderBatchID = gb.GraderBatchID 
/* Input Field Bins*/ 
LEFT JOIN
    (
	SELECT 
		GraderBatchID, 
		SUM(bu.BinQty) AS NoOfFieldBins, 
		SUM(bu.BinQty * (bd.TotalWeight / bd.NoOfBins)) AS BinKgs
    FROM   ma_Bin_UsageT AS bu 
	INNER JOIN
		ma_Bin_DeliveryT AS bd 
	ON bd.BinDeliveryID = bu.BinDeliveryID
	WHERE  bd.PresizeFlag = 0
	GROUP BY GraderBatchID
	) AS bu 
ON bu.GraderBatchID = gb.GraderBatchID 
/* Input Presize Bins*/ 
LEFT JOIN
	(
	SELECT 
		GraderBatchID, 
		SUM(bu.BinQty) AS NoOfPresizeBins, 
		SUM(bu.BinQty * (bd.TotalWeight / bd.NoOfBins)) AS BinKgs
    FROM ma_Bin_UsageT bu 
	INNER JOIN
        ma_Bin_DeliveryT bd 
	ON bd.BinDeliveryID = bu.BinDeliveryID
    WHERE  bd.PresizeFlag = 1
    GROUP BY GraderBatchID
	) bp 
ON bp.GraderBatchID = gb.GraderBatchID 
/*-- Output Presize Bins (Not including Juice)*/ 
LEFT JOIN
    (
	SELECT 
		bd.PresizeOutputFromGraderBatchID, 
		SUM(bd.TotalWeight) AS PresizeWeight
    FROM   ma_Bin_DeliveryT AS bd 
	INNER JOIN
		(
		SELECT 
			pt.ProductID,
			pt.NetFruitWeight,
			pt.TubesPerCarton,
			pt.SampleFlag,
			gt.JuiceFlag,
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
		INNER JOIN
			(
			SELECT
				GradeID,
				JuiceFlag
			FROM sw_GradeT
			) AS gt
		ON gt.GradeID = pt.GradeID
		) AS pr
	ON pr.ProductID = bd.PresizeProductID 
    WHERE  pr.JuiceFlag = 0
    GROUP BY bd.PresizeOutputFromGraderBatchID
	) AS pbo 
ON pbo.PresizeOutputFromGraderBatchID = gb.GraderBatchID 
/*-- Output Juice */ 
LEFT JOIN
    (
	SELECT 
		bd.PresizeOutputFromGraderBatchID, 
		SUM(bd.TotalWeight) AS JuiceWeight
    FROM ma_Bin_DeliveryT AS bd 
	INNER JOIN
		(
		SELECT 
			pt.ProductID,
			pt.NetFruitWeight,
			pt.TubesPerCarton,
			pt.SampleFlag,
			gt.JuiceFlag,
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
		INNER JOIN
			(
			SELECT
				GradeID,
				JuiceFlag
			FROM sw_GradeT
			) AS gt
		ON gt.GradeID = pt.GradeID
		) AS pr
	ON pr.ProductID = bd.PresizeProductID 
    WHERE  pr.JuiceFlag = 1
    GROUP BY bd.PresizeOutputFromGraderBatchID
	) jbo 
ON jbo.PresizeOutputFromGraderBatchID = gb.GraderBatchID 
/* Sample Cartons*/ 
LEFT JOIN
    (
	SELECT 
		pdm.GraderBatchID, 
		SUM(pdm.Kgs) AS SampleWeight
    FROM 
		(
		SELECT 
			pd.PalletDetailID, 
			pd.PalletID, 
			pd.ProductID, 
			COALESCE (pd.GraderBatchID, re.GraderBatchID) AS GraderBatchID, 
			pd.NoOfUnits, 
			pd.NoOfUnits * pr.TubesPerCarton AS Tubes, 
			CAST(CASE 
					WHEN 
						ebd.ExportBinKGWeight IS NOT NULL THEN (ebd.ExportBinKGWeight / pr.PresizeAvgTubeWeight) * pr.RTEConversion 
					ELSE 
						pd.NoOfUnits * pr.TubesPerCarton * pr.RTEConversion 
					END AS numeric(10, 2)) AS RTEs, 
			CAST(CASE 
					WHEN 
						ebd.ExportBinKGWeight IS NOT NULL THEN ebd.ExportBinKGWeight 
					ELSE 
						ISNULL(pd.TotalCartonWeightNet, 0) + 
						(CASE 
							WHEN 
								pd.TotalCartonWeightNet IS NOT NULL THEN ISNULL(pd.NoOfUnitsUnweighed, 0) 
							ELSE pd.NoOfUnits 
						 END * pr.NetFruitWeight) 
					END AS numeric(10, 2)) AS Kgs
		FROM ma_Pallet_DetailT AS pd 
		INNER JOIN
			(
			SELECT
				ProductID,
				TubesPerCarton,
				tt.RTEConversion,
				tt.PresizeAvgTubeWeight,
				NetFruitWeight,
				SampleFlag
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
			ma_RepackT AS re 
		ON re.RepackID = pd.OutputFromRepackID 
		LEFT OUTER JOIN
			(
			SELECT 
				PalletDetailID, 
				SUM(KGWeight) AS ExportBinKGWeight
			FROM ma_Export_Bin_DetailT
			GROUP BY PalletDetailID
			) AS ebd 
		ON ebd.PalletDetailID = pd.PalletDetailID
		WHERE pr.SampleFlag = 1
		) AS pdm
	GROUP BY pdm.GraderBatchID
	) AS sam 
ON sam.GraderBatchID = gb.GraderBatchID




