import au.com.bytecode.opencsv.CSVReader;
import org.jetbrains.annotations.NotNull;

import java.io.FileReader;
import java.io.IOException;
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

            ResultSetMetaData EmployeeSetMetaData = selectedEmployeeSet.getMetaData ();
            System.out.println ("getColumnName :"+EmployeeSetMetaData.getColumnName (1));
            System.out.println ("getColumnLabel :"+EmployeeSetMetaData.getColumnLabel (1));
            System.out.println ("getColumnTypeName :"+EmployeeSetMetaData.getColumnTypeName (1));
            System.out.println ("getCatalogName :"+EmployeeSetMetaData.getCatalogName (1));
            System.out.println ("getCatalogName :"+EmployeeSetMetaData.getTableName (1));

            String  str = "INSERT INTO employees_transformed () VALUES ("+"FIRST_NAME, LAST_NAME, EMAIL, HIRE_DATE, SALARY, DEPARTMENT_ID, MANAGER_ID, JOB_TITLE, MIN_SALARY, MAX_SALARY, EMPLOYEE_ID, COMMISSION_PCT)";


            connection.commit ();
            connection.close ();
            System.out.println ("Connections closed.");
        } catch (Exception exception) {
            exception.printStackTrace ();
        }
    }

    private static void insertResultRow(Connection connection) throws SQLException {
        //Create a Statement for scrollable ResultSet
        PreparedStatement prepStmt = connection.prepareStatement(
                "SELECT * FROM employees_transformed",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);

// Catch the ResultSet object
        ResultSet res = prepStmt.executeQuery ();

// Check ResultSet's updatability
        if (res.getConcurrency () == ResultSet.CONCUR_READ_ONLY) {
            System.out.println ("ResultSet non-updatable.");
        } else {
            System.out.println ("ResultSet updatable.");
        }

// Move the cursor to the insert row
        res.moveToInsertRow ();
//
//// Set the new first name and last name
//            res.updateString ("FirstName", "Lucy");
//            res.updateString ("LastName", "Harrington");
//            res.updateString ("Point", "123.456");
//            res.updateString ("BirthDate", "1977-07-07");
//            res.updateString ("ModTime", "2007-01-01 01:01:01.001");

// Store the insert into database
        res.insertRow ();

// Move the cursor back to the current row
        res.moveToCurrentRow ();

        System.out.println ("Row inserted ok.");

// Close ResultSet and Statement
        res.close ();
        prepStmt.close ();
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection ("jdbc:mysql://localhost:3306/mydb?" +
                "user=root&password=&serverTimezone=UTC");
    }

    private static void addCountryTable(Connection connection) throws SQLException {
        PreparedStatement statement;
        String sql;
        connection.createStatement ().executeUpdate ("DELETE FROM country");
        sql = "INSERT INTO country() values(?,?,?,?,?,?,?,?,?,?);";
        statement = connection.prepareStatement (sql);
        consumeCountryCSVandSetParam (statement, "src/csv_data_sources/COUNTRIES.csv");
    }

    private static void consumeCountryCSVandSetParam(PreparedStatement statement, String filePath) throws SQLException {
        try (CSVReader reader = new CSVReader (new FileReader (filePath),
                ',', '"', 1)) {
            String[] nextLine;
            while ((nextLine = reader.readNext ()) != null) {
                if (nextLine != null) {
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
            }
        } catch (IOException e) {
            e.printStackTrace ();
        }
    }

    private static void addEmployeeTable(Connection connection) throws SQLException {
        PreparedStatement statement;
        String sql;
        connection.createStatement ().executeUpdate ("DELETE FROM employee");
        sql = "INSERT INTO employee () values(?,?,?,?,?,?,?,?,?,?,?,?);";
        statement = connection.prepareStatement (sql);
        consumeEmployeeCSVandSetParam (statement, "src/csv_data_sources/Employees.csv");
    }

    private static void consumeEmployeeCSVandSetParam(PreparedStatement statement, String filePath) throws SQLException {

        try (CSVReader reader = new CSVReader (new FileReader (filePath),
                ',', '"', 1)) {
            String[] nextLine;
            while ((nextLine = reader.readNext ()) != null) {
                if (nextLine != null) {
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
            }
        } catch (IOException e) {
            e.printStackTrace ();
        }

    }

    private static void addDepartmentTable(Connection connection) throws SQLException, IOException {
        String sql;
        PreparedStatement statement;
        connection.createStatement ().executeUpdate ("DELETE FROM Department"); // Free Table data.
        sql = "insert into Department () values(?,?,?,?,?);";
        statement = connection.prepareStatement (sql);
        consumeDepartmentCSVandSetParam (statement, "src/csv_data_sources/Departments.csv");
    }

    private static void consumeDepartmentCSVandSetParam(PreparedStatement statement, String filePath) throws IOException, SQLException {
        try (CSVReader reader = new CSVReader (new FileReader (filePath),
                ',', '"', 1)) {
            String[] nextLine;
            while ((nextLine = reader.readNext ()) != null) {
                if (nextLine != null) {
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
            }
        } catch (IOException e) {
            e.printStackTrace ();
        }
    }

    @NotNull
    private static ResultSet getSelectedEmployeeSet(Connection connection) throws SQLException {
        String selectEmployeesSql = "SELECT * FROM employee WHERE MIN_SALARY >= 5500";
        ResultSet selectedEmployeeSet = connection.createStatement ().executeQuery (selectEmployeesSql);
        return selectedEmployeeSet;
    }

    private static void displayResultSet(ResultSet selectedEmployeeSet) throws SQLException {
        System.out.println ("Employees having MIN_SALARY >= 5500 :");
        while (selectedEmployeeSet.next ()) {
            System.out.print ("ID: " + selectedEmployeeSet.getInt ("EMPLOYEE_ID"));
            System.out.print (",  Name: " + selectedEmployeeSet.getString ("FIRST_NAME"));
            System.out.print (", Last Name: " + selectedEmployeeSet.getString ("LAST_NAME"));
            System.out.println (", MIN_SALARY: " + selectedEmployeeSet.getDouble ("MIN_SALARY"));
        }
    }

    private static void createTableForResultSet(Connection connection, ResultSet selectedEmployeeSet) throws SQLException {
        ResultSetMetaData EmployeeSetMetaData = selectedEmployeeSet.getMetaData ();
        String createTableQuery = "CREATE TABLE IF NOT EXISTS Employees_Transformed (";
        createTableQuery += buildTableColumn (EmployeeSetMetaData);
        createTableQuery = createTableQuery.replace ("DOUBLE(22)", "DOUBLE"); //Filter query

        System.out.println ("Query to create new table: " + createTableQuery);
        connection.createStatement ().execute (createTableQuery);
        System.out.println ("Table created .......");
    }

    private static String buildTableColumn(ResultSetMetaData resultSetMetaData) throws SQLException {
        int columnCount = resultSetMetaData.getColumnCount ();
        String createQuery = "";
        for (int i = 1; i < columnCount; i++) {
            createQuery +=
                    resultSetMetaData.getColumnName (i) +
                            " " + resultSetMetaData.getColumnTypeName (i)
                            + "(" + resultSetMetaData.getPrecision (i) + "), ";
        }
        //add last column and close parentheses
        createQuery +=
                resultSetMetaData.getColumnName (columnCount) +
                        " " + resultSetMetaData.getColumnTypeName (columnCount)
                        + "(" + resultSetMetaData.getPrecision (columnCount) + ") );";
        return createQuery;
    }
}
