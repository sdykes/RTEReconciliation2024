library(tidyverse)

################################################################################
#                           Read the Sunfruit data                             #
################################################################################

SunfruitTable <- function(Sunfruitfile,MasterData) {

  SFPO241121 <- read_csv({{Sunfruitfile}}, show_col_types = F) |>
    filter(BinSource == "Orchard pick") |>
    mutate(PackDate = as.Date(PackDate, "%d/%m/%Y")) |>
    select(-c(BinSource, ContractVendor, RunState, TCEsExport, TCEsLocal, 
              TCEsProcess, KGsLocal, AvgSizeDenominator,AvgSizeNumerator)) |>
    mutate(PackOut = KGsExport/KGsTotal,
           ProductionSite = str_sub(OrchardProdSite, 6,-1)) |>
    relocate(ProductionSite, .after = OrchardProdSite)

  con <- DBI::dbConnect(odbc::odbc(),    
                        Driver = "ODBC Driver 18 for SQL Server", 
                        Server = "abcrepldb.database.windows.net",  
                        Database = "ABCPackerRepl",   
                        UID = "abcadmin",   
                        PWD = "Trauts2018!",
                        Port = 1433
  )

  BinDelivery <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLFiles2024/BinDeliveryRaw.sql"))

  BinsTransferredInV2 <- DBI::dbGetQuery(con,
                                         "SELECT 
	                                            tt.TransferID,
	                                            btt.BinDeliveryID,
	                                            btt.NoOfBinsTransferredIn
                                          FROM ma_TransferT AS tt
                                          INNER JOIN
	                                            (
	                                            SELECT 
		                                              TransferID,
		                                              bt.BinDeliveryID,
		                                              COUNT(tbt.BinID) AS NoOfBinsTransferredIn
	                                            FROM ma_Transfer_BinT AS tbt
	                                        INNER JOIN
		                                          (
		                                          SELECT
			                                            BinID,
			                                            BinDeliveryID,
			                                            StorageSiteCompanyID
		                                          FROM ma_BinT
		                                          ) AS bt
	                                        ON tbt.BinID = bt.BinID
	                                        GROUP BY TransferID, BinDeliveryID 
	                                        ) AS btt
                                          ON tt.TransferID = btt.TransferID
                                          WHERE ToStorageSiteCompanyID = 477"
                                        )

  BinsTransferredOutV2 <- DBI::dbGetQuery(con,
                                         "SELECT 
	                                            tt.TransferID,
	                                            btt.BinDeliveryID,
	                                            btt.NoOfBinsTransferredOut
                                          FROM ma_TransferT AS tt
                                          INNER JOIN
	                                            (
	                                            SELECT 
		                                              TransferID,
		                                              bt.BinDeliveryID,
		                                              COUNT(tbt.BinID) AS NoOfBinsTransferredOut
	                                            FROM ma_Transfer_BinT AS tbt
	                                            INNER JOIN
		                                              (
		                                              SELECT
			                                                BinID,
			                                                BinDeliveryID,
			                                                StorageSiteCompanyID
		                                              FROM ma_BinT
		                                              ) AS bt
	                                            ON tbt.BinID = bt.BinID
	                                            GROUP BY TransferID, BinDeliveryID 
	                                            ) AS btt
                                          ON tt.TransferID = btt.TransferID
                                          WHERE FromStorageSiteCompanyID = 477"
                                        )

  BinsPackedByBinDeliveryID <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2024/BinsPackedByBinDeliveryID.sql"))


  DBI::dbDisconnect(con)

  SFBinreco <- BinDelivery |>
    filter(`Storage site` == "Sunfruit Limited") |>
    full_join(BinsTransferredInV2 |> rename(TransferIn = TransferID), by = "BinDeliveryID") |>
    full_join(BinsTransferredOutV2 |> rename(TransferOut = TransferID), by = "BinDeliveryID") |>
    mutate(across(.cols = c(NoOfBins, NoOfBinsTransferredIn, NoOfBinsTransferredOut), ~replace_na(.,0))) |>
    mutate(NoOfBinsReco = NoOfBins + NoOfBinsTransferredIn - NoOfBinsTransferredOut) |>
    select(c(BinDeliveryID,NoOfBinsReco)) |>
    left_join(BinDelivery |> select(-NoOfBins), by = "BinDeliveryID") |>
    rename(NoOfBins = NoOfBinsReco) |>
    left_join(BinsPackedByBinDeliveryID, by = "BinDeliveryID") |>
    filter(NoOfBins != 0,
           !is.na(RPIN)) 

  SFRunReco <- SFBinreco |>
    mutate(Run = case_when(BinDeliveryID %in% c(12518,11080,13295) ~ "P1569",
                           BinDeliveryID %in% c(12156,11909,12157) ~ "P1430",
                           BinDeliveryID %in% c(12027,12028,11908) ~ "P1429",
                           BinDeliveryID %in% c(12029,14188) ~ "P1573",
                           BinDeliveryID %in% c(10549,10619,10592) ~ "P1459",
                           BinDeliveryID == 11578 ~ "P1420",
                           BinDeliveryID == 11473 ~ "P1148",
                           BinDeliveryID == 12163 ~ "P1453",
                           BinDeliveryID %in% c(12199,12316,12086,12119,12087,12197,12262) ~ "P1574",
                           BinDeliveryID == 12034 ~ "P1423",
                           BinDeliveryID == 10692 ~ "P1490",
                           BinDeliveryID == 12162 ~ "P1421",
                           BinDeliveryID == 12164 ~ "P1422",
                           BinDeliveryID %in% c(11784,10752) ~ "P1482",
                           BinDeliveryID %in% c(11769,10765) ~ "P1248",
                           BinDeliveryID %in% c(10727,10726) ~ "P1249",
                           BinDeliveryID %in% c(11632,11633) ~ "P1484",
                           BinDeliveryID %in% c(10495,10453,10489,10510,10521,10488,10494) ~ "P1465",
                           BinDeliveryID == 10555 ~ "P1460",
                           BinDeliveryID %in% c(11768,11783) ~ "P1483",
                           BinDeliveryID %in% c(11856,11855) ~ "P1287",
                           BinDeliveryID %in% c(13274,14454,14108,13282,13359,13467,13279,13311,14468) ~ "P1576",
                           BinDeliveryID %in% c(14453,13469,13199) ~ "P1575",
                           BinDeliveryID == 11854 ~ "P1424",
                           BinDeliveryID == 13315 ~ "P1578",
                           BinDeliveryID %in% c(14106,12125,14086,13200) ~ "P1577",
                           BinDeliveryID %in% c(13693,13691,13576) ~ "P1199", # Not an exact match adds to 58 bins
                           BinDeliveryID %in% c(10537,10603,10604,10601,10559,10602,10519,10535,10560,10538) ~ "P1466",
                           BinDeliveryID %in% c(11771,11486,11489,11490,13529) ~ "P1255",
                           BinDeliveryID %in% c(11487,11488,11491,13530,13553,13657,13659) ~ "P1254",
                           BinDeliveryID %in% c(13655,13656) ~ "P1258", # Does not reconcile only 38 bins
                           BinDeliveryID == 13552 ~ "P1452",
                           BinDeliveryID %in% c(11884,13527,13528,13551,13658) ~ "P1291",
                           BinDeliveryID %in% c(14279,14280,14365) ~ "P1198",
                           BinDeliveryID %in% c(14366,14380) ~ "P1382", # 63 bins here
                           BinDeliveryID %in% c(11764,11916) ~ "P1481",
                           BinDeliveryID %in% c(12719,10513) ~ "P1480",
                           BinDeliveryID %in% c(11600,11604,11648) ~ "P1197",
                           BinDeliveryID %in% c(10511,10543,10515,10546,11601,11603,11709,11778,11780,11799) ~ "P1479",
                           BinDeliveryID %in% c(11882,11883,11941,11942,11945,11946) ~ "P1427",
                           BinDeliveryID %in% c(11881,11944) ~ "P1288",
                           BinDeliveryID %in% c(13568,13569,13668,13670,13714) ~ "P1331",
                           BinDeliveryID %in% c(13711,13715,13849) ~ "P1404",
                           BinDeliveryID %in% c(11805,11829) ~ "P1283",
                           BinDeliveryID %in% c(11887,13850) ~ "P1286",
                           BinDeliveryID %in% c(10523,10597,10512,10577,10571,10525) ~ "P1463",
                           BinDeliveryID %in% c(10598,10639) ~ "P1462",
                           BinDeliveryID %in% c(10626,10621,10624,10650,10653,11374,10625,11302) ~ "P1469",
                           BinDeliveryID %in% c(10652,10651,10668,10579,10622) ~ "P1470",
                           BinDeliveryID %in% c(10666,10574,10623,10600,10640,10578,10641,10572,10599) ~ "P1461",
                           BinDeliveryID == 11866 ~ "P1488",
                           BinDeliveryID == 11772 ~ "P1489",
                           BinDeliveryID %in% c(13176,13000,13001) ~ "P1544",
                           BinDeliveryID %in% c(11654,13567,13706,13839,11827) ~ "P1383", # 239 bins instead of 241
                           BinDeliveryID %in% c(11867,12149) ~ "P1428", # 23 bins instead of 21
                           BinDeliveryID == 13608 ~ "P1457",
                           BinDeliveryID %in% c(14121,14196,14122,14114) ~ "P1565",
                           BinDeliveryID %in% c(14268,14270,14273,14278,14325,14328,14332,14340,14362) ~ "P1221",
                           BinDeliveryID %in% c(13980,13984) ~ "P1319",
                           BinDeliveryID %in% c(13875,13982,14207,14269,13874,13996,14006,14015,14205,14206,14222,14263) ~ "P1301",
                           BinDeliveryID %in% c(13724,13834,13833,14114,14122,14196,14121) ~ "P1563",
                           BinDeliveryID %in% c(13981,13983,13994,13995,14004,14005,14016,14017,14018,14272,14326,14329,14331,14338,14339,14363) ~ "P1332",
                           BinDeliveryID %in% c(14155,14135,14194,14062,14115,14136,14195,14123,14120,14116,14061) ~ "P1566",
                           BinDeliveryID %in% c(10646,10696,11828) ~ "P1485",
                           BinDeliveryID == 11576 ~ "P1316",
                           BinDeliveryID == 14113 ~ "P1568",
                           BinDeliveryID == 12513 ~ "P1567",
                           BinDeliveryID %in% c(11899,12147) ~ "P1476",
                           BinDeliveryID == 13309 ~ "P1513",
                           BinDeliveryID %in% c(13584,13908,11544) ~ "P1571",
                           BinDeliveryID == 11804 ~ "P1491",
                           BinDeliveryID == 11702 ~ "P1145",
                           BinDeliveryID == 11888 ~ "P1409",
                           BinDeliveryID == 11704 ~ "P1146",
                           BinDeliveryID == 13475 ~ "P1219",
                           BinDeliveryID %in% c(11752,11776,13694) ~ "P1456",
                           BinDeliveryID %in% c(11753,13772,13844,13845,13898) ~ "P1408",
                           BinDeliveryID %in% c(12246,12152,12136) ~ "P1142",
                           BinDeliveryID == 12715 ~ "P1454",
                           BinDeliveryID == 13663 ~ "P1405",
                           BinDeliveryID %in% c(11493,12713) ~ "P1396",
                           BinDeliveryID %in% c(11492,11803) ~ "P1397",
                           BinDeliveryID == 12111 ~ "P1478",
                           BinDeliveryID == 11442 ~ "P1519",
                           BinDeliveryID %in% c(11851, 12112) ~ "P1477",
                           BinDeliveryID == 13959 ~ "P1393",
                           BinDeliveryID %in% c(12493,12255,12293,12333) ~ "P1520",
                           BinDeliveryID %in% c(11886,13997,14014) ~ "P1392",
                           BinDeliveryID %in% c(11809,11852,12110) ~ "P1475",
                           BinDeliveryID == 12943 ~ "P1570",
                           BinDeliveryID %in% c(11975,12180) ~ "P1426",
                           BinDeliveryID %in% c(11589,13088,12866) ~ "P1141",
                           BinDeliveryID %in% c(12339,12929,13039,12927,13038,11976,12665,11086,11443,11833,12130,11304) ~ "P1583",
                           BinDeliveryID %in% c(11337) ~ "P1414",
                           BinDeliveryID %in% c(11306,12251,12006,11338,11084,11444) ~ "P1523",
                           BinDeliveryID %in% c(11336,12252,11445,12490,11085) ~ "P1522",
                           BinDeliveryID == 14343 ~ "P1451",
                           BinDeliveryID %in% c(11830,11973,12181) ~ "P1413",
                           BinDeliveryID == 13090 ~ "P1147",
                           BinDeliveryID %in% c(13322,13372,13430) ~ "P1218",
                           BinDeliveryID %in% c(13855,14209,14357,14401,14416) ~ "P1346",
                           BinDeliveryID == 13229 ~ "P1315",
                           BinDeliveryID %in% c(13803,13703,13807,14059) ~ "P1584",
                           BinDeliveryID %in% c(14284,14341,14360,14400,14417) ~ "P1394",
                           BinDeliveryID %in% c(13852,13762) ~ "P1547",
                           BinDeliveryID %in% c(13185,13230) ~ "P1152",
                           BinDeliveryID == 13401 ~ "P1252",
                           BinDeliveryID %in% c(13574,13575,13637,13638,13639,13640,13641,13642,13687,13688,13690,13820,13821,13858,13933,13935,13964,1400,14007) ~ "P1344",
                           BinDeliveryID %in% c(12481,12249) ~ "P1521",
                           BinDeliveryID %in% c(13822,13857,13934,13965,13966,13998,13999,14001,14008) ~ "P1345",
                           BinDeliveryID %in% c(10557,11299,10647,11417,10596,10654) ~ "P1464",
                           BinDeliveryID == 11279 ~ "P1486",
                           BinDeliveryID %in% c(11390,11277,10616,10648,11318) ~ "P1524",
                           BinDeliveryID == 11610 ~ "P1487",
                           BinDeliveryID %in% c(10594,10595,10558,10527,10536) ~ "P1458",
                           BinDeliveryID %in% c(10649,10700,10676,10662) ~ "P1467",
                           BinDeliveryID == 11865 ~ "P1471",
                           BinDeliveryID %in% c(10716,11631,11767,11781,11718,11864) ~ "P1317",
                           BinDeliveryID %in% c(10716,11631,11767,11781,11718,11864) ~ "P1473",
                           BinDeliveryID %in% c(10738,10762,10719,10702) ~ "P1473",
                           BinDeliveryID %in% c(11653,11671) ~ "P1151",
                           BinDeliveryID == 11939 ~ "P1150",
                           BinDeliveryID %in% c(10744,10690,10750,12032,12161,12035) ~ "P1474",
                           BinDeliveryID %in% c(10689,10741,10742,12036,10747,10753,12033) ~ "P1472",
                           BinDeliveryID %in% c(12530,12499,11111,12531,11100,12666,11109,12563) ~ "P1516",
                           BinDeliveryID %in% c(12564,12532,11110,12500) ~ "P1515",
                           BinDeliveryID %in% c(12509,11307,12312,11236,12287,11245,11092,11606) ~ "P1517",
                           BinDeliveryID %in% c(11467,11432) ~ "P1143",
                           BinDeliveryID == 11357 ~ "P1144",
                           BinDeliveryID == 11602 ~ "P1492",
                           BinDeliveryID %in% c(11599,11650) ~ "P1398",
                           BinDeliveryID %in% c(11649,11651,11713,11715,11774,11798,11807,11818) ~ "P1318",
                           BinDeliveryID %in% c(11717,11718,11775,11797,11806) ~ "P1285",
                           BinDeliveryID %in% c(13620,13689,13621,13768) ~ "P1543",
                           BinDeliveryID == 13774 ~ "P1546",
                           BinDeliveryID == 13972 ~ "P1280", 
                           BinDeliveryID == 13823 ~ "P1548",
                           BinDeliveryID %in% c(13616,13617,13731,13827,13829) ~ "P1200",
                           BinDeliveryID %in% c(11789,13696) ~ "P1257",
                           BinDeliveryID == 11850 ~ "P1289",
                           BinDeliveryID %in% c(11794,11822,11868) ~ "P1256",
                           BinDeliveryID %in% c(11790,11792,11820,11821,11848,11849,11869,11870,11876) ~ "P1118",
                           BinDeliveryID %in% c(13732,13825,13828,13830,13879,13923) ~ "P1253",
                           BinDeliveryID %in% c(13619,13697,13698,13699,13728,13729,13730,13826,13831,13878,13880,13882,13924,13925,13926,13928,13951,13968,13969,13970,14159,14293) ~ "P1302",
                           BinDeliveryID %in% c(12724,12628) ~ "P1579",
                           BinDeliveryID %in% c(14158,12778,12727,14157,12726,12995) ~ "P1549",
                           BinDeliveryID %in% c(12904,12624,14162,12626,14160,12623,12559) ~ "P1580",
                           BinDeliveryID %in% c(14233,14234,14235,14291,14292,14295,14296,14305,14353,14399) ~ "P1320",
                           BinDeliveryID %in% c(14297,14306,14351,14352) ~ "P1303",
                           BinDeliveryID %in% c(12996,13362,13363,13134,13526,12905,12960,12959,12901,13525) ~ "P1564",
                           BinDeliveryID == 12987 ~ "P1455",
                           BinDeliveryID %in% c(12781,13133,12902,12782,12906,12997,13364,13029,12958) ~ "P1581",
                           BinDeliveryID %in% c(12990,12779,12728,12777,12998,12755) ~ "P1582",
                           BinDeliveryID %in% c(13565,13612,13613,13676,13677,13701,13793,13796,13847,14344) ~ "P1379",
                           BinDeliveryID == 13431 ~ "P1220",
                           BinDeliveryID == 13537 ~ "P1545",
                           BinDeliveryID %in% c(13491,13450) ~ "P1251",
                           BinDeliveryID %in% c(13566,13614,13615,13674,13797,13846) ~ "P1410",
                           BinDeliveryID %in% c(13564,13675,13700,13766) ~ "P1411",
                           BinDeliveryID %in% c(13451,13432) ~ "P1250",
                           BinDeliveryID %in% c(10760,11591,11520) ~ "P1314",
                           BinDeliveryID %in% c(11825,11815) ~ "P1140",
                           BinDeliveryID == 11831 ~ "P1401",
                           BinDeliveryID %in% c(12492,12376) ~ "P1313",
                           BinDeliveryID %in% c(11832,11974,12177,12178,12179) ~ "P1425",
                           BinDeliveryID %in% c(11446,12253) ~ "P1514",
                           BinDeliveryID %in% c(11519,13854) ~ "P1149",
                           BinDeliveryID %in% c(14213,14214,14281) ~ "P1399",
                           BinDeliveryID == 13470 ~ "P1247",
                           BinDeliveryID == 11788 ~ "P1282",
                           BinDeliveryID == 11791 ~ "P1431",
                           BinDeliveryID == 11874 ~ "P1412",
                           BinDeliveryID %in% c(11793,11873) ~ "P1468",
                           BinDeliveryID %in% c(11810,13885) ~ "P1284",
                           BinDeliveryID == 11811 ~ "P1290",
                           BinDeliveryID == 11812 ~ "P1402",
                           BinDeliveryID == 11846 ~ "P1403",
                           BinDeliveryID %in% c(14190,14142,14137) ~ "P1551",
                           BinDeliveryID == 14175 ~ "P1572",
                           BinDeliveryID %in% c(14143,14189,14103) ~ "P1550",
                           BinDeliveryID %in% c(13819,13877,13894) ~ "P1281",
                           BinDeliveryID %in% c(13920,13944,14164) ~ "P1381",
                           BinDeliveryID %in% c(13883,13922,14165) ~ "P1377",
                           BinDeliveryID %in% c(13649,13798,13816,13876) ~ "P1378",
                           BinDeliveryID %in% c(13650,13792,13795,13817,13921) ~ "P1380",
                           BinDeliveryID %in% c(11842,13512,13643) ~ "P1395",
                           TRUE ~ "Unassigned")) 

  RunSummary <- SFRunReco |>
    group_by(Run) |>
#  
# The next statement averages the BindDelivery details across the run using weighted means 
#  
    summarise(HarvestDate = weighted.mean(HarvestDate, NoOfBins, na.rm=T),
              MaturityID = round(weighted.mean(MaturityID, NoOfBins, na.rm=T),0),
              StorageTypeID = round(weighted.mean(StorageTypeID, NoOfBins, na.rm=T),0),
              PickNoID = round(weighted.mean(PickNoID, NoOfBins, na.rm=T),0),
              NoOfBins = sum(NoOfBins),) |>
    mutate(StorageTypeID = if_else(StorageTypeID == 5,4,StorageTypeID),
           StorageType = case_when(StorageTypeID == 4 ~ "CA Smartfresh",
                                   StorageTypeID == 6 ~ "Normal Air",
                                   StorageTypeID == 7 ~ "Smartfreshed"),
          MaturityCode = case_when(MaturityID == 1 ~ "A",
                                   MaturityID == 2 ~ "B",
                                   MaturityID == 3 ~ "C",
                                   MaturityID == 4 ~ "D",
                                   MaturityID == 6 ~ "I",
                                   MaturityID == 7 ~ "X",
                                   MaturityID == 13 ~ "N"),
          PickNo = case_when(PickNoID == 2 ~ "1st Pick",
                             PickNoID == 3 ~ "2nd Pick",
                             PickNoID == 4 ~ "3rd Pick",
                             PickNoID == 5 ~ "4th Pick",
                             PickNoID == 6 ~ "5th Pick")
    ) |> 
    filter(Run != "Unassigned") |>
    select(-c(StorageTypeID))

RunReco <- SFPO241121 |>
  rename(StorageTypeSF = StorageType) |>
  left_join(RunSummary, by = "Run") |>
  mutate(StorageDays = as.integer(PackDate - HarvestDate),
         HarvestDays = yday(HarvestDate),
         StorageTypeSF = case_when(StorageTypeSF == "Smartfresh CA" ~ "CA Smartfresh",
                                 StorageTypeSF == "Smartfresh Air" ~ "Smartfreshed",
                                 TRUE ~ "Normal Air")) |>
  select(-c(OrchardBlock, Orchard, KGsExport, KGsNonProcess, StorageType)) |>
  rename(FarmCode = OrchardPIN,
         InputKgs = KGsTotal,
         RejectKgs = KGsProcess,
         StorageType = StorageTypeSF) |>
  select(c(Run,
           PackDate,
           HarvestDate,
           FarmCode,      
           ProductionSite,
           MaturityCode,
           StorageType,   
           PickNo,
           Bins,
           InputKgs,
           RejectKgs,
           StorageDays,
           PackOut))

  TestTable <- RunReco |>
    left_join({{MasterData}}, by = c("FarmCode","ProductionSite")) |>
    select(-c(FarmName,RowSpacing,TreeSpacing,YearPlanted,QtyTrees,
              QtyRows,RejectKgs, ManagerName)) |>
    mutate(HarvestDays = yday(HarvestDate))

#write_csv(TestTable, "FinalSFTable.csv")

  return(TestTable)
}

TestTable <- SunfruitTable("data/SunFruitPackOuts241121.csv",MasterDataByPS)

FinalTestTable <- TestTable |>
  select(-c(Run, Bins, Orchard, FarmCode, ProductionSite, Owner, PackDate, HarvestDate)) |>
  rename(`Storage type` = StorageType) |>
  filter(!is.na(PackOut))

#write_csv(FinalTestTable, "FinalTestSet.csv")
  





