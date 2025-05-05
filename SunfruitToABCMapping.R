library(tidyverse)
  

SFToABCMapping <- function(PackoutFile) {

  SFPO241121 <- read_csv("data/SunFruitPackOuts241121.csv", show_col_types = F) |>
    filter(BinSource == "Orchard pick") |>
    mutate(PackDate = as.Date(PackDate, "%d/%m/%Y")) |>
    select(-c(BinSource, ContractVendor, RunState, TCEsExport, TCEsLocal, 
          TCEsProcess, KGsLocal, AvgSizeDenominator,AvgSizeNumerator)) |>
    mutate(Packout = KGsExport/KGsTotal,
          ProductionSite = str_sub(OrchardProdSite, 6,-1)) |>
    relocate(ProductionSite, .after = OrchardProdSite) |>
    select(-c(OrchardProdSite))

  con <- DBI::dbConnect(odbc::odbc(),    
                        Driver = "ODBC Driver 18 for SQL Server", 
                        Server = "abcrepldb.database.windows.net",  
                        Database = "ABCPackerRepl",   
                        UID = "abcadmin",   
                        PWD = "Trauts2018!",
                        Port = 1433
  )

  GBDInputKgMapping <- DBI::dbGetQuery(con,
                                       "SELECT 
	                                          GraderBatchID,
	                                          InputKgs
                                        FROM ma_Grader_BatchT
                                        WHERE SeasonID = 2011
                                        AND PresizeInputFlag = 0
                                        AND PackingCompanyID = 477")

  DBI::dbDisconnect(con)

  MappedInputBins <- SFPO241121 |>
    mutate(GraderBatchID = case_when(Run %in% c("P1462","P1469") ~ 6179,
                                     Run %in% c("P1424","P1575","P1576") ~ 6094,
                                     Run %in% c("P1430","P1569") ~ 6122,
                                     Run == "P1283" ~ 5627,
                                     Run == "P1284" ~ 5624,
                                     Run == "P1286" ~ 5640,
                                     Run %in% c("P1291", "P1258", "P1452") ~ 5579,
                                     Run == "P1344" ~ 5784,
                                     Run %in% c("P1256", "P1289") ~ 5580,
                                     Run %in% c("P1393", "P1477") ~ 5977,
                                     Run %in% c("P1281", "P1381") ~ 5626,
                                     Run %in% c("P1221","P1301","P1319","P1563","P1565") ~ 5470,
                                     Run %in% c("P1332","P1566") ~ 5750,
                                     Run == "P1409" ~ 6019,
                                     Run %in% c("P1476","P1513","P1567","P1568") ~ 6210,
                                     Run %in% c("P1383","P1428","P1457") ~ 5893,
                                     Run %in% c("P1147","P1218","P1315","P1346") ~ 5409,
                                     Run == "P1331" ~ 5751,
                                     Run == "P1142" ~ 5250,
                                     Run == "P1282" ~ 5639,
                                     Run == "P1285" ~ 5638,
                                     Run %in% c("P1250","P1410","P1411") ~ 5576,
                                     Run == "P1378" ~ 5882,
                                     Run == "P1377" ~ 5881,
                                     Run %in% c("P1220","P1251","P1379","P1545") ~ 5457,
                                     Run == "P1380" ~ 5894,
                                     Run == "P1145" ~ 5255,
                                     Run == "P1287" ~ 5689,
                                     Run %in% c("P1221","P1301","P1319","P1563","P1565") ~ 5470,
                                     Run %in% c("P1200", "P1257") ~ 5408,
                                     Run == "P1290" ~ 5691,
                                     Run %in% c("P1118","P1253") ~ 5214,
                                     Run == "P1254" ~ 5578,
                                     Run == "P1288" ~ 5690,
                                     Run %in% c("P1280","P1548") ~ 5625,
                                     Run == "P1395" ~ 5981,
                                     Run %in% c("P1302","P1579") ~ 5692,
                                     Run == "P1402" ~ 5979,
                                     Run == "P1403" ~ 5980,
                                     Run == "P1255" ~ 5577,
                                     Run %in% c("P1144","P1492") ~ 5406,
                                     Run %in% c("P1197","P1480") ~ 5403,
                                     Run %in% c("P1491","P1571") ~ 6313,
                                     Run %in% c("P1345","P1521") ~ 5790,
                                     Run == "P1318" ~ 5737,
                                     Run %in% c("P1198","P1382") ~ 5404,
                                     Run == "P1466" ~ 6193,
                                     Run == "P1453" ~ 6181,
                                     Run %in% c("P1303","P1320","P1549","P1580") ~ 5714,
                                     Run %in% c("P1413","P1451","P1522") ~ 6052,
                                     Run %in% c("P1146","P1219","P1456") ~ 5407,
                                     Run == "P1398" ~ 5973,
                                     Run == "P1429" ~ 6123,
                                     Run == "P1397" ~ 5976,
                                     Run %in% c("P1316","P1485") ~ 5725,
                                     Run == "P1404" ~ 5982,
                                     Run == "P1396" ~ 5975,
                                     Run %in% c("P1140","P1314","P1401") ~ 5245,
                                     Run %in% c("P1248","P1482") ~ 5574,
                                     Run %in% c("P1247","P1399") ~ 5581,
                                     Run == "P1199" ~ 5402,
                                     Run %in% c("P1405","P1454") ~ 5974,
                                     Run == "P1546" ~ 6518,
                                     Run == "P1463" ~ 6178,
                                     Run == "P1488" ~ 6310,
                                     Run %in% c("P1478","P1519") ~ 6314,
                                     Run %in% c("P1149","P1313","P1425","P1514") ~ 5411,
                                     Run %in% c("P1458","P1487") ~ 6183,
                                     Run == "P1465" ~ 6176,
                                     Run == "P1408" ~ 6020,
                                     Run %in% c("P1464","P1486","P1524") ~ 6182,
                                     Run == "P1572" ~ 6832,
                                     Run %in% c("P1148","P1420","P1459") ~ 5253,
                                     Run %in% c("P1249","P1484") ~ 5575,
                                     Run == "P1550" ~ 6516,
                                     Run %in% c("P1143","P1517") ~ 5249,
                                     Run %in% c("P1141","P1414","P1426","P1523","P1583") ~ 5244,
                                     Run %in% c("P1152","P1252","P1394","P1547","P1584") ~ 5410,
                                     Run == "P1544" ~ 6514,
                                     Run == "P1573" ~ 6831,
                                     Run == "P1287" ~ 6879, # may be an issue with this one!!
                                     Run == "P1427" ~ 6124,
                                     Run == "P1551" ~ 6515,
                                     Run %in% c("P1392","P1475","P1520","P1570") ~ 5978,
                                     Run == "P1516" ~ 6347,
                                     Run == "P1472" ~ 6211,
                                     Run == "P1479" ~ 6309,
                                     Run %in% c("P1461","P1470") ~ 6180,
                                     Run == "P1460" ~ 6177,
                                     Run == "P1543" ~ 6517,
                                     Run == "P1422" ~ 6096,
                                     Run == "P1481" ~ 6311,
                                     Run %in% c("P1455","P1564") ~ 6184,
                                     Run == "P1467" ~ 6200,
                                     Run %in% c("P1421","P1423","P1490","P1574") ~ 6095,
                                     Run == "P1483" ~ 6308,
                                     Run == "P1489" ~ 6312,
                                     Run == "P1515" ~ 6348,
                                     Run == "P1581" ~ 6880, # 2000 kg disparity between SF and ABC
                                     Run == "P1582" ~ 6921,
                                     Run %in% c("P1151","P1317","P1471","P1473") ~ 5405,
                                     Run %in% c("P1577","P1578") ~ 6879,
                                     Run %in% c("P1150","P1474") ~ 5254,
                                     Run %in% c("P1412","P1468","P1431") ~ 6051, # Disparity in the Grader_Batch
                                     TRUE ~ 0)) 

  #SFPOByGBD <- MappedInputBins |>
  #  group_by(GraderBatchID) |>
  #  summarise(Bins = sum(Bins),
  #            KGsTotal = sum(KGsTotal),
  #            KGsExport = sum(KGsExport)) |>
  #  mutate(Packout = KGsExport/KGsTotal)

  return(MappedInputBins)
}

MappedInputBins <- SFToABCMapping("data/SunFruitPackOuts241121.csv")
