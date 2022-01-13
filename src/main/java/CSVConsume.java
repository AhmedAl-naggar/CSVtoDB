import au.com.bytecode.opencsv.CSVReader;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.*;
import java.sql.*;

import static java.lang.Integer.parseInt;
import static java.lang.Double.parseDouble;

public class CSVConsume {

    public static void main(String[] args) {
        try {


            Connection connection = getConnection ();
            connection.setAutoCommit (false);

            addDepartmentTable (connection);
            addEmployeeTable (connection);
            addCountryTable (connection);

            ResultSet selectedEmployeeSet = getSelectedEmployeeSet (connection);

            displayResultSet (selectedEmployeeSet);
            createTableForResultSet (connection, selectedEmployeeSet);
            generateInsertQuery (selectedEmployeeSet, connection);

            connection.close ();
            System.out.println ("Connections closed.");
        } catch (Exception exception) {
            exception.printStackTrace ();
        }
    }

    private static void createTableIfNotExist(Connection connection,String filePath) throws FileNotFoundException {
        //Initialize the script runner
        ScriptRunner sr = new ScriptRunner(connection);
        //Creating a reader object
        Reader reader = new BufferedReader (new FileReader(filePath));
        //Running the script
        sr.runScript(reader);
    }


    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection ("jdbc:mysql://localhost:3306/mydb?" +
                "user=root&password=&serverTimezone=UTC");
    }

    private static void addCountryTable(Connection connection) throws SQLException, FileNotFoundException {
        createTableIfNotExist (connection, "src/tables_Creation_mysql_files/country.sql");
        PreparedStatement statement;
        String sql;
        freeTableRows (connection, "country");
        sql = "INSERT INTO country() values(?,?,?,?,?,?,?,?,?,?);";
        statement = connection.prepareStatement (sql);
        consumeCountryCSVandSetParam (statement);
        connection.commit ();
    }
    private static void consumeCountryCSVandSetParam(PreparedStatement statement) throws SQLException {
        try (CSVReader reader = new CSVReader (new FileReader ("src/csv_data_sources/COUNTRIES.csv"),
                ',', '"', 1)) {
            String[] nextLine;
            while ((nextLine = reader.readNext ()) != null) {
                String COUNTRY_ID = nextLine[0];
                String COUNTRY_ISO_CODE = nextLine[1];
                String COUNTRY_NAME = nextLine[2];
                String COUNTRY_SUBREGION = nextLine[3];
                String COUNTRY_SUBREGION_ID = nextLine[4];
                String COUNTRY_REGION = nextLine[5];
                String COUNTRY_REGION_ID = nextLine[6];
                String COUNTRY_TOTAL = nextLine[7];
                String COUNTRY_TOTAL_ID = nextLine[8];
                String COUNTRY_NAME_HIST = nextLine[9];

                if (COUNTRY_ID.length () != 0)
                    statement.setString (1, COUNTRY_ID);
                else
                    statement.setNull (1, Types.INTEGER);

                statement.setString (2, COUNTRY_ISO_CODE);
                statement.setString (3, COUNTRY_NAME);
                statement.setString (4, COUNTRY_SUBREGION);

                if (COUNTRY_SUBREGION_ID.length () != 0)
                    statement.setString (5, COUNTRY_SUBREGION_ID);
                else
                    statement.setNull (5, Types.INTEGER);

                statement.setString (6, COUNTRY_REGION);

                if (COUNTRY_REGION_ID.length () != 0)
                    statement.setString (7, COUNTRY_REGION_ID);
                else
                    statement.setNull (7, Types.INTEGER);

                statement.setString (8, COUNTRY_TOTAL);

                if (COUNTRY_TOTAL_ID.length () != 0)
                    statement.setString (9, COUNTRY_TOTAL_ID);
                else
                    statement.setNull (9, Types.INTEGER);

                if (COUNTRY_NAME_HIST.length () != 0)
                    statement.setString (10, COUNTRY_NAME_HIST);
                else
                    statement.setNull (10, Types.VARCHAR);

                statement.addBatch ();
                statement.executeBatch ();
            }
        } catch (IOException e) {
            e.printStackTrace ();
        }
    }

    private static void addEmployeeTable(Connection connection) throws SQLException, FileNotFoundException {
        createTableIfNotExist (connection, "src/tables_Creation_mysql_files/employee.sql");
        PreparedStatement statement;
        String sql;
        freeTableRows (connection, "employee");
        sql = "INSERT INTO employee () values(?,?,?,?,?,?,?,?,?,?,?,?);";
        statement = connection.prepareStatement (sql);
        consumeEmployeeCSVandSetParam (statement);
        connection.commit ();
    }

    private static void consumeEmployeeCSVandSetParam(PreparedStatement statement) throws SQLException {

        try (CSVReader reader = new CSVReader (new FileReader ("src/csv_data_sources/Employees.csv"),
                ',', '"', 1)) {
            String[] nextLine;
            while ((nextLine = reader.readNext ()) != null) {
                String FIRST_NAME = nextLine[0];
                String LAST_NAME = nextLine[1];
                String EMAIL = nextLine[2];
                String HIRE_DATE = nextLine[3];
                String SALARY = nextLine[4];
                String DEPARTMENT_ID = nextLine[5];
                String MANAGER_ID = nextLine[6];
                String JOB_TITLE = nextLine[7];
                String MIN_SALARY = nextLine[8];
                String MAX_SALARY = nextLine[9];
                String EMPLOYEE_ID = nextLine[10];
                String COMMISSION_PCT = nextLine[11];

                statement.setString (1, FIRST_NAME);
                statement.setString (2, LAST_NAME);
                statement.setString (3, EMAIL);
                statement.setString (4, HIRE_DATE);

                if (SALARY.length () != 0)
                    statement.setDouble (5, Double.parseDouble (SALARY));
                else
                    statement.setNull (5, Types.DOUBLE);

                if (DEPARTMENT_ID.length () != 0)
                    statement.setInt (6, parseInt (DEPARTMENT_ID));
                else
                    statement.setNull (6, Types.INTEGER);

                if (MANAGER_ID.length () != 0)
                    statement.setInt (7, parseInt (MANAGER_ID));
                else
                    statement.setNull (7, Types.INTEGER);

                statement.setString (8, JOB_TITLE);

                if (MIN_SALARY.length () != 0)
                    statement.setDouble (9, parseDouble (MIN_SALARY));
                else
                    statement.setNull (9, Types.DOUBLE);

                if (MAX_SALARY.length () != 0)
                    statement.setDouble (10, parseDouble (MAX_SALARY));
                else
                    statement.setNull (10, Types.DOUBLE);


                if (EMPLOYEE_ID.length () != 0)
                    statement.setInt (11, parseInt (EMPLOYEE_ID));
                else
                    statement.setNull (11, Types.INTEGER);


                if (COMMISSION_PCT.length () != 0)
                    statement.setDouble (12, Double.parseDouble (COMMISSION_PCT));
                else
                    statement.setNull (12, Types.DOUBLE);

                statement.addBatch ();
                statement.executeBatch ();

//                    SimpleDateFormat sd1 = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
//                    Date dt = sd1.parse (HIRE_DATE);
//                    SimpleDateFormat sd2 = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
//                    String newDate = sd2.format (dt);
//                    System.out.println (newDate);
            }
        } catch (IOException e) {
            e.printStackTrace ();
        }

    }

    private static void addDepartmentTable(Connection connection) throws SQLException, FileNotFoundException {
        createTableIfNotExist (connection, "src/tables_Creation_mysql_files/department.sql");
        String sql;
        PreparedStatement statement;
        freeTableRows (connection, "Department");
        sql = "insert into Department () values(?,?,?,?,?);";
        statement = connection.prepareStatement (sql);
        consumeDepartmentCSVandSetParam (statement);
        connection.commit ();
    }
    private static void consumeDepartmentCSVandSetParam(PreparedStatement statement) throws SQLException {
        try (CSVReader reader = new CSVReader (new FileReader ("src/csv_data_sources/Departments.csv"),
                ',', '"', 1)) {
            String[] nextLine;
            while ((nextLine = reader.readNext ()) != null) {
                String MANAGER_ID = nextLine[0];
                String DEPARTMENT_NAME = nextLine[1];
                String MGR_FIRST_NAME = nextLine[2];
                String DEPARTMENT_ID = nextLine[3];
                String MGR_LAST_NAME = nextLine[4];

                if (MANAGER_ID.length () != 0)
                    statement.setInt (1, parseInt (MANAGER_ID));
                else
                    statement.setNull (1, Types.INTEGER);

                statement.setString (2, DEPARTMENT_NAME);
                statement.setString (3, MGR_FIRST_NAME);

                if (DEPARTMENT_ID.length () != 0)
                    statement.setInt (4, parseInt (DEPARTMENT_ID));
                else
                    statement.setNull (4, Types.INTEGER);

                statement.setString (5, MGR_LAST_NAME);

                statement.addBatch ();
                statement.executeBatch ();
            }
        } catch (IOException e) {
            e.printStackTrace ();
        }
    }

    private static ResultSet getSelectedEmployeeSet(Connection connection) throws SQLException {
        String selectEmployeesSql = "SELECT * FROM employee WHERE MIN_SALARY >= 5500";
        return connection.createStatement (
                ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE).executeQuery (selectEmployeesSql);
    }

    private static void displayResultSet(ResultSet selectedEmployeeSet) throws SQLException {
        System.out.println (getResultSetSize (selectedEmployeeSet) + " Employees having MIN_SALARY >= 5500 :");
        while (selectedEmployeeSet.next ()) {
            System.out.print ("ID: " + selectedEmployeeSet.getInt ("EMPLOYEE_ID"));
            System.out.print (",  Name: " + selectedEmployeeSet.getString ("FIRST_NAME"));
            System.out.print (", MANAGER_ID: " + selectedEmployeeSet.getInt ("MANAGER_ID"));
            System.out.println (", MIN_SALARY: " + selectedEmployeeSet.getDouble ("MIN_SALARY"));
        }
        selectedEmployeeSet.beforeFirst ();
    }

    private static void generateInsertQuery(ResultSet selectedEmployeeSet, Connection connection) throws SQLException {
        freeTableRows (connection, "employees_transformed");
        String insertToTableQuery = "INSERT INTO employees_transformed () VALUES (";
        ResultSetMetaData employeeSetMetaData = selectedEmployeeSet.getMetaData ();
        int columnCount = employeeSetMetaData.getColumnCount ();

        while (selectedEmployeeSet.next ()) {
            StringBuilder createQuery = new StringBuilder ();
            for (int i = 1; i < columnCount; i++) {
                var value = selectedEmployeeSet.getString (employeeSetMetaData.getColumnName (i));
                if (isNumber (value)) {
                    createQuery.append (value).append (", ");

                } else if (!isNullValue (value)) {
                    createQuery.append ("\"").append (value).append ("\", ");
                } else {
                    createQuery.append (value).append (", ");
                }
            }

            var value = selectedEmployeeSet.getString (employeeSetMetaData.getColumnName (columnCount));
            //add last column and close parentheses
            if (isNumber (value)) {
                createQuery.append (value).append (");");
            } else if (!isNullValue (value)) {
                createQuery.append ("\"").append (value).append ("\" );");
            } else {
                createQuery.append (value).append (" );");
            }
            insertToTableQuery += createQuery;
            connection.createStatement ().execute (insertToTableQuery);
            insertToTableQuery = "INSERT INTO employees_transformed () VALUES (";
        }
        System.out.println ("Data inserted into the table");
        selectedEmployeeSet.beforeFirst ();
        connection.commit ();
    }

    private static void createTableForResultSet(Connection connection, ResultSet selectedEmployeeSet) throws SQLException {
        ResultSetMetaData employeeSetMetaData = selectedEmployeeSet.getMetaData ();
        String createTableQuery = "CREATE TABLE IF NOT EXISTS Employees_Transformed (";
        createTableQuery += buildTableColumn (employeeSetMetaData);
        createTableQuery = createTableQuery.replace ("DOUBLE(22)", "DOUBLE"); //Filter query

        System.out.println ("Query to create Employees_Transformed table: " + createTableQuery);
        connection.createStatement ().execute (createTableQuery);
        connection.commit ();
        System.out.println ("Employees_Transformed Table created .......");
    }

    private static String buildTableColumn(ResultSetMetaData resultSetMetaData) throws SQLException {
        int columnCount = resultSetMetaData.getColumnCount ();
        StringBuilder createQuery = new StringBuilder ();
        for (int i = 1; i < columnCount; i++) {
            createQuery.append (resultSetMetaData.getColumnName (i)).append (" ")
                    .append (resultSetMetaData.getColumnTypeName (i)).append ("(")
                    .append (resultSetMetaData.getPrecision (i)).append ("), ");
        }
        //add last column and close parentheses
        createQuery.append (resultSetMetaData.getColumnName (columnCount)).append (" ").append (resultSetMetaData.getColumnTypeName (columnCount)).append ("(").append (resultSetMetaData.getPrecision (columnCount)).append (") );");
        return createQuery.toString ();
    }

    private static int getResultSetSize(ResultSet selectedEmployeeSet) throws SQLException {
        int size = 0;
        if (selectedEmployeeSet != null) {
            selectedEmployeeSet.last ();    // moves cursor to the last row
            size = selectedEmployeeSet.getRow (); // get row id
            selectedEmployeeSet.beforeFirst ();    // moves cursor to the last row
        }
        return size;
    }

    private static void freeTableRows(Connection connection, String tableName) throws SQLException {
        connection.createStatement ().executeUpdate ("DELETE FROM " + tableName); // Free Table data.
        connection.commit ();
    }

    private static boolean isNumber(String str) {
        try {
            if (str != null) {
                Double.parseDouble (str);
                return true;
            }
        } catch (NumberFormatException e) {
            return false;
        }
        return false;
    }

    private static boolean isNullValue(String str) {
        try {
            if (str.equals ("")) {
                return true;
            }
        } catch (NullPointerException e) {
            return true;
        }
        return false;
    }
}
