SELECT 
	bd.PresizeOutputFromGraderBatchID AS GraderBatchID, 
	gb.PresizeMarketID, bd.PresizeProductID, 
	bd.TotalWeight AS Kgs, 
	CAST(ROUND(bd.TotalWeight / pr.PresizeAvgTubeWeight, 0) AS int) AS NoOfTubes, 
	CAST(ROUND(bd.TotalWeight / pr.PresizeAvgTubeWeight, 0) * pr.RTEConversion AS numeric(10, 2)) AS RTEs, 
	gb.SeasonID, 
	gb.SubdivisionID, 
	sb.SubdivisionCode, 
	gb.MaturityID
FROM  ma_Bin_DeliveryT AS bd 
INNER JOIN
	ma_Grader_BatchT AS gb 
ON gb.GraderBatchID = bd.PresizeOutputFromGraderBatchID 
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
LEFT OUTER JOIN
    sw_SubdivisionT AS sb 
ON sb.SubdivisionID = gb.SubdivisionID
WHERE (bd.PresizeFlag = 1) 
AND (pr.JuiceFlag = 0)

