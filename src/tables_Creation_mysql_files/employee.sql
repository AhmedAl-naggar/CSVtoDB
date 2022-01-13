CREATE TABLE IF NOT EXISTS  `employee` (
  `FIRST_NAME` varchar(45) NOT NULL,
  `LAST_NAME` varchar(45) NOT NULL,
  `EMAIL` varchar(45) NOT NULL,
  `HIRE_DATE` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_croatian_ci NOT NULL,
  `SALARY` double NOT NULL,
  `DEPARTMENT_ID` int(11) NOT NULL,
  `MANAGER_ID` int(11) DEFAULT NULL,
  `JOB_TITLE` varchar(45) NOT NULL,
  `MIN_SALARY` double NOT NULL,
  `MAX_SALARY` double NOT NULL,
  `EMPLOYEE_ID` int(11) NOT NULL,
  `COMMISSION_PCT` double DEFAULT NULL,
  PRIMARY KEY (`EMPLOYEE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
