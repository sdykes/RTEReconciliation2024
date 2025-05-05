SELECT 
	bd.BinDeliveryID,
	ft.FarmCode,
	ft.FarmName,
	bpn.BlockCode,
	bpn.ProductionSite,
	bd.HarvestDate,
	bd.NoOfBins,
	ct.StorageSite,
	mt.RSP,
	stt.StorageTypeDesc,
	pknt.PickNoDesc
FROM ma_Bin_DeliveryT AS bd
INNER JOIN
     (
     SELECT
        FarmID,
        FarmCode,
        FarmName
     FROM sw_FarmT
      ) AS ft
ON bd.FarmID = ft.FarmID
INNER JOIN
	(
    SELECT
		fbt.BlockID,
        fbt.BlockCode,
        st.ProductionSite
    FROM sw_Farm_BlockT AS fbt
	INNER JOIN
		(
		SELECT 
			SubdivisionID,
			SubdivisionCode AS ProductionSite
		FROM sw_SubdivisionT
		) AS st
	ON fbt.SubdivisionID = st.SubdivisionID
    ) AS bpn
ON bd.BlockID = bpn.BlockID
INNER JOIN
    (
    SELECT
       CompanyID,
       CompanyName AS StorageSite
    FROM sw_CompanyT
    ) AS ct
ON bd.FirstStorageSiteCompanyID = ct.CompanyID
INNER JOIN
    (
    SELECT
        MaturityID,
        MaturityCode AS RSP
    FROM sw_MaturityT
    ) AS mt
ON bd.MaturityID = mt.MaturityID
INNER JOIN
    (
    SELECT
       StorageTypeID,
       StorageTypeDesc
    FROM sw_Storage_TypeT
    ) AS stt
ON bd.StorageTypeID = stt.StoragetypeID 
INNER JOIN
    (
    SELECT
       PickNoID,
       PickNoDesc
    FROM sw_Pick_NoT
    ) AS pknt
ON bd.PickNoID = pknt.PickNoID
WHERE PresizeFlag = 0

