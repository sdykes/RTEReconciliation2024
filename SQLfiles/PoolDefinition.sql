SELECT 
	ProductID,
	ProductDesc,
	PoolDesc
FROM sw_ProductT AS pt
INNER JOIN
	(
	SELECT
		TubeTypeID,
		PoolDesc 
	FROM sw_Tube_TypeT AS tt
	INNER JOIN
		sw_Tube_DiameterT AS tdt
	ON tdt.TubeDiameterID = tt.TubeDiameterID
	INNER JOIN
		fi_PoolT AS fpt
	ON fpt.TubeDiameterID = tdt.TubeDiameterID
	) AS ttt
ON ttt.TubeTypeID = pt.TubeTypeID

