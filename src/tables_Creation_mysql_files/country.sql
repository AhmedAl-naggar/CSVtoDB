CREATE TABLE IF NOT EXISTS `country` (
  `COUNTRY_ID` int(11) NOT NULL,
  `COUNTRY_ISO_CODE` varchar(2) NOT NULL,
  `COUNTRY_NAME` varchar(45) NOT NULL,
  `COUNTRY_SUBREGION` varchar(45) NOT NULL,
  `COUNTRY_SUBREGION_ID` int(11) NOT NULL,
  `COUNTRY_REGION` varchar(45) NOT NULL,
  `COUNTRY_REGION_ID` int(11) NOT NULL,
  `COUNTRY_TOTAL` varchar(45) NOT NULL,
  `COUNTRY_TOTAL_ID` int(11) NOT NULL,
  `COUNTRY_NAME_HIST` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
