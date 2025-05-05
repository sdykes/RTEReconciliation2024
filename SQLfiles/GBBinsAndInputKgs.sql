SELECT 
	gb.GraderBatchID,
	ABCBins,
	InputKgs
FROM ma_Grader_BatchT AS gb
INNER JOIN
	(
	SELECT 
		GraderBatchID,
		COUNT(BinID) AS ABCBins
	FROM ma_BinT
	GROUP BY GraderBatchID
	) AS bt
ON bt.GraderBatchID = gb.GraderBatchID