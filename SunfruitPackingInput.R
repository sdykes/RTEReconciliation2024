library(tidyverse)

SFPO241121 <- read_csv("data/SunFruitPackOuts241121.csv", show_col_types = F) |>
  filter(BinSource == "Orchard pick") |>
  mutate(PackDate = as.Date(PackDate, "%d/%m/%Y")) |>
  select(-c(BinSource, ContractVendor, RunState, TCEsExport, TCEsLocal, 
            TCEsProcess, KGsLocal, AvgSizeDenominator,AvgSizeNumerator)) |>
  mutate(Packout = KGsExport/KGsTotal,
         ProductionSite = str_sub(OrchardProdSite, 6,-1)) |>
  relocate(ProductionSite, .after = OrchardProdSite)


################################################################################
#             Get the bin delivery information from ABC                        #
################################################################################

getSQL <- function(filepath){
  con = file(filepath, "r")
  sql.string <- ""
  
  while (TRUE){
    line <- readLines(con, n = 1)
    
    if ( length(line) == 0 ){
      break
    }
    
    line <- gsub("\\t", " ", line)
    
    if(grepl("--",line) == TRUE){
      line <- paste(sub("--","/*",line),"*/")
    }
    
    sql.string <- paste(sql.string, line)
  }
  
  close(con)
  return(sql.string)
}

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

BinDelivery <- DBI::dbGetQuery(con,"SQLFiles/BinDelivery.sql")

BinsTransferredIn <- DBI::dbGetQuery(con,
                                   "/* Total query is for Bin transferred into Sunfruit */
                                    /* BinID to BinDeliveryID mapping for non presize bins */
                                    SELECT
	                                      tt2.BinDeliveryID,
	                                      SUM(tt2.NoOfBinsTransferred)/COUNT(tt2.TransferID) AS NoOfBinsTransferredIn
                                    FROM 	
	                                      (
	                                      SELECT 
		                                        tt.TransferID,
		                                        bd2.BinDeliveryID,
		                                        bd2.NoOfBinsTransferred,
		                                        tt.FromStorageSiteCompanyID,
		                                        tt.ToStorageSiteCompanyID,
		                                        tt.TransferDate
	                                      FROM ma_TransferT AS tt
	                                      LEFT JOIN
		                                    (
		                                    SELECT
			                                      tbt.TransferID,
			                                      bd.BinDeliveryID,
			                                      COUNT(bt.BinID) AS NoOfBinsTransferred
		                                    FROM ma_Bin_DeliveryT AS bd
		                                    INNER JOIN
			                                      (
			                                      SELECT 
				                                        BinID,
				                                        BinDeliveryID
			                                      FROM ma_BinT
			                                      WHERE GraderBatchID IS NOT NULL
			                                      ) AS bt
		                                    ON bd.BinDeliveryID = bt.BinDeliveryID
		                                    INNER JOIN
			                                      (
			                                      SELECT
				                                        BinID,
				                                        TransferID
			                                      FROM ma_Transfer_BinT
			                                      ) tbt
		                                    ON bt.BinID = tbt.BinID
		                                    WHERE PresizeFlag = 0
		                                    GROUP BY tbt.TransferID, bd.BinDeliveryID
		                                    ) AS bd2
	                                  ON tt.TransferID = bd2.TransferID
	                                  WHERE ToStorageSiteCompanyID = 477
	                                  ) AS tt2
                                GROUP BY tt2.BinDeliveryID"
                                )

BinsTransferredOut <- DBI::dbGetQuery(con,
                                      "/* Total query is for Bin transferred out of Sunfruit */
                                      /* BinID to BinDeliveryID mapping for non presize bins */
                                      SELECT
	                                        tt2.BinDeliveryID,
	                                        SUM(tt2.NoOfBinsTransferred)/COUNT(tt2.TransferID) AS NoOfBinsTransferredOut
                                      FROM 	
	                                        (
	                                        SELECT 
		                                          tt.TransferID,
		                                          bd2.BinDeliveryID,
		                                          bd2.NoOfBinsTransferred,
		                                          tt.FromStorageSiteCompanyID,
		                                          tt.ToStorageSiteCompanyID,
		                                          tt.TransferDate
	                                        FROM ma_TransferT AS tt
	                                        INNER JOIN
		                                          (
		                                          SELECT
			                                            tbt.TransferID,
			                                            bd.BinDeliveryID,
			                                            COUNT(bt.BinID) AS NoOfBinsTransferred
		                                          FROM ma_Bin_DeliveryT AS bd
		                                          INNER JOIN
			                                            (
			                                            SELECT 
				                                              BinID,
				                                              BinDeliveryID
			                                            FROM ma_BinT
			                                            WHERE GraderBatchID IS NOT NULL
			                                            ) AS bt
		                                          ON bd.BinDeliveryID = bt.BinDeliveryID
		                                          INNER JOIN
			                                            (
			                                            SELECT
				                                              BinID,
				                                              TransferID
			                                            FROM ma_Transfer_BinT
			                                            ) tbt
		                                          ON bt.BinID = tbt.BinID
		                                          WHERE PresizeFlag = 0
		                                          GROUP BY tbt.TransferID, bd.BinDeliveryID
		                                          ) AS bd2
	                                        ON tt.TransferID = bd2.TransferID
	                                        WHERE FromStorageSiteCompanyID = 477
	                                        ) AS tt2
                                      GROUP BY tt2.BinDeliveryID"
                                      )

DBI::dbDisconnect(con)

#
# Bins originally taken to Sunfruit from orchard
#
BinDeliverySF <- BinDelivery |>
  filter(StorageSite == "Sunfruit Limited")
#
#BinDeliveryTransferred <- BinsTransFerred |>
#  left_join(BinDelivery |>
#              select(-NoOfBins), by = "BinDeliveryID") |>
#  rename(NoOfBins = NoOfBinsTransferred)
#
# The BinDeliveryIDs that have been transferred in twice - take the final transferredIn bin No
#
test_inner <- inner_join(BinsTransferredIn, BinDeliverySF, by = "BinDeliveryID") |>
  select(-NoOfBins) |>
  rename(NoOfBins = NoOfBinsTransferredIn) |>
  relocate(NoOfBins, .after = HarvestDate)
#
# The rest of the Transferred Batches and the existing batches combined
#
test <- full_join(BinsTransferredIn, BinDeliverySF, by = "BinDeliveryID") |>
  mutate(NoOfBins = coalesce(NoOfBinsTransferredIn, NoOfBins)) |>
#
# Remove the double up these will be replaced with the test_inner dataframe
#
  filter(!(!is.na(NoOfBinsTransferredIn) & !is.na(FarmCode))) |>
  select(-NoOfBinsTransferredIn) |>
  bind_rows(test_inner)
# This list all existing bins that were orginally at SF
test_existing <- test |>
  filter(!is.na(FarmCode))
#
# This is all the bins that have been transferred in (and run)
#
test_TranferredIn <- test |>
  filter(is.na(FarmCode)) |>
  select(c(BinDeliveryID,NoOfBins)) |>
  rename(NoOfBinsInterim = NoOfBins) |>
  left_join(BinDelivery, by = "BinDeliveryID") |>
  select(-NoOfBins) |>
  rename(NoOfBins = NoOfBinsInterim) |>
  relocate(NoOfBins, .after = HarvestDate) |>
  bind_rows(test_existing)
#
# This is the final data frame which includes bins originally at SF + bins transferred in - bins transferred out
#
test2 <- left_join(test_TranferredIn, BinsTransferredOut, by = "BinDeliveryID") |>
  mutate(NoOfBinsTransferredOut = replace_na(NoOfBinsTransferredOut,0),
         BinsToProcess = NoOfBins-NoOfBinsTransferredOut) |>
  select(-c(NoOfBins,NoOfBinsTransferredOut)) |>
  rename(NoOfBins = BinsToProcess) |>
  relocate(NoOfBins, .after = HarvestDate) |>
  filter(NoOfBins != 0)


Test3 <- test2 |>
  mutate(Run = case_when(BinDeliveryID %in% c(12518,11080,13295) ~ "P1569",
                         BinDeliveryID %in% c(12156,11909,12157) ~ "P1430",
                         BinDeliveryID %in% c(12027,12028,11908) ~ "P1429",
                         BinDeliveryID %in% c(12029,14188) ~ "P1573",
                         BinDeliveryID %in% c(10540,10619,10592) ~ "P1459",
                         BinDeliveryID == 11578 ~ "P1420",
                         BinDeliveryID == 11473 ~ "P1148",
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
                         BinDeliveryID %in% c(10603,10537,10566,10601,10535,10538,10604,10559,10519,10602) ~ "P1466",
                         BinDeliveryID %in% c(11771,11486,11489,11490,13529) ~ "P1255",
                         BinDeliveryID %in% c(11487,11488,11491,13530,13553,13657,13659) ~ "P1254",
                         BinDeliveryID %in% c(13655,13656,13936,13950) ~ "P1258", # Does not reconcile
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
                         BinDeliveryID %in% c(10579,10523,10577,10512,10525,10571) ~ "P1463",
                         BinDeliveryID %in% c(10598,10639) ~ "P1462",
                         BinDeliveryID %in% c(10652,10651,10668,10579,10622) ~ "P1470",
                         BinDeliveryID %in% c(10666,10574,10623,10600,10640,10578,10641,10572,10599) ~ "P1461",
                         BinDeliveryID == 11866 ~ "P1488",
                         BinDeliveryID == 11772 ~ "P1489",
                         BinDeliveryID %in% c(13176,13000,13001) ~ "P1544",
                         BinDeliveryID %in% c(11654,13567,13706,13839,11827) ~ "P1383",
                         BinDeliveryID %in% c(11867,12149) ~ "P1428",
                         BinDeliveryID == 13608 ~ "P1457",
                         BinDeliveryID %in% c(14121,14196,14122,14114) ~ "P1565",
                         BinDeliveryID %in% c(13875,13982,14207,14269,13874,13980) ~ "P1221",
                         BinDeliveryID %in% c(13996,14006,14325,14362) ~ "P1319",
                         BinDeliveryID %in% c(14015,14205,14206,14222,14263,14268,14270,14273,14278,14328,14332) ~ "P1301",
                         BinDeliveryID %in% c(13724,13822,13834) ~ "P1563",
                         BinDeliveryID %in% c(13981,13983,13995,14004,14018,14331,14338,14363,13994,14005,14016,14017,14272,14326,14329,14339) ~ "P1332",
                         BinDeliveryID %in% c(14195,14115,14136,14062,14061,14155,14135,14116,14190,14120,14123) ~ "P1566",
                         BinDeliveryID %in% c(10646,10696,11828) ~ "P1485",
                         BinDeliveryID == 11576 ~ "P1316",
                         BinDeliveryID == 14113 ~ "P1568",
                         BinDeliveryID == 12513 ~ "P1567",
                         BinDeliveryID %in% c(11899,12147) ~ "P1476",
                         BinDeliveryID == 13309 ~ "P1513",
                         BinDeliveryID %in% c(13584,13908,11544) ~ "P1517",
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
                         BinDeliveryID %in% c(11589,12866,13088) ~ "P1141",
                         BinDeliveryID %in% c(12339,12927,12929,13038,13039,11976,11084,11086,11304,11306,11338,11443) ~ "P1583",
                         BinDeliveryID == 11337 ~ "P1414",
                         BinDeliveryID %in% c(11444,11833,12006,12130,12251,12665) ~ "P1523",
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
                         BinDeliveryID %in% c(10716,11631,11767,11781,11718,11864) ~ "P1473",
                         BinDeliveryID %in% c(10738,10762,10719,10702) ~ "P1473",
                         BinDeliveryID %in% c(11653,11671) ~ "P1151",
                         BinDeliveryID == 11939 ~ "P1150",
                         BinDeliveryID %in% c(10744,10690,10750,12032,12161,12035) ~ "P1474",
                         BinDeliveryID %in% c(10689,10741,10742,12036,10747,10753,12033) ~ "P1472",
                         BinDeliveryID %in% c(12530,12499,11111,12531,11100,12666,11109,12563) ~ "P1516",
                         BinDeliveryID %in% c(12564,12532,11110,12500) ~ "P1515",
                         BinDeliveryID %in% c(12509,11307,12312,11236,12287,11245,11092,11606) ~ "P1517",
                         BinDeliveryID == 11357 ~ "P1144",
                         BinDeliveryID == 11602 ~ "P1492",
                         BinDeliveryID %in% c(11599,11650) ~ "P1398",
                         BinDeliveryID %in% c(11649,11651,11713,11715,11774,11798,11807,11818) ~ "P1318",
                         BinDeliveryID %in% c(11717,11718,11775,11797,11806) ~ "P1285",
                         BinDeliveryID %in% c(13620,13689,13621,13768) ~ "P1543",
                         BinDeliveryID == 13724 ~ "P1546",
                         BinDeliveryID == 13972 ~ "P1280", 
                         BinDeliveryID == 13823 ~ "P1548",
                         BinDeliveryID %in% c(13616,13617,13731,13827,13829) ~ "P1200",
                         BinDeliveryID %in% c(11789,13696) ~ "P1257",
                         BinDeliveryID == 11850 ~ "P1257",
                         BinDeliveryID %in% c(11794,11822,11868) ~ "P1256",
                         BinDeliveryID %in% c(11790,11792,11820,11821,11848,11849,11869,11870,11876) ~ "P1118",
                         BinDeliveryID %in% c(13732,13825,13828,13830,13879,13923) ~ "P1253",
                         BinDeliveryID %in% c(13619,13697,13698,13699,13728,13729,13730,13826,13831,13878,13880,13882,13924,13925,13926,13928,13951,13968,13969,13970,14159,14293) ~ "P1302",
                         BinDeliveryID %in% c(12724,12628) ~ "P1529",
                         BinDeliveryID %in% c(14158,12778,12727,14157,12726,12995) ~ "P1549",
                         BinDeliveryID %in% c(12904,12624,14162,12626,14160,12623,12559) ~ "P1580",
                         BinDeliveryID %in% c(14233,14234,14235,14291,14292,14295,14296,14305,14353,14399) ~ "P1320",
                         BinDeliveryID %in% c(14297,14306,14351,14352) ~ "P1303",
                         BinDeliveryID %in% c(12996,13362,13363,13134,13526,12905,12960,12959,12901,13525) ~ "P1564",
                         BinDeliveryID == 14294 ~ "P1455",
                         BinDeliveryID %in% c(12781,13133,12902,12782,12906,12997,13364,13029,12958) ~ "P1581",
                         BinDeliveryID %in% c(12990,12779,12728,12777,12998,12755) ~ "P1582",
                         BinDeliveryID %in% c(13565,13612,13613,13676,13677,13701,13793,13796,13847,14344) ~ "P1379",
                         BinDeliveryID == 13431 ~ "P1222",
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
 
RunSummary <- Test3 |>
  group_by(Run) |>
  summarise(NoOfBins = sum(NoOfBins))

RunReco <- SFPO241121 |>
  left_join(RunSummary, by = "Run") |>
  filter(is.na(NoOfBins))
  


