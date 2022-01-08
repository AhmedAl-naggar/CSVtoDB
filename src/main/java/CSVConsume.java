
import Models.Employee;
import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.bean.ColumnPositionMappingStrategy;
import au.com.bytecode.opencsv.bean.CsvToBean;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.lang.Integer.parseInt;
import static java.lang.Double.parseDouble;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.util.Calendar.PM;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CSVConsume {

    //@SuppressWarnings({"rawtypes", "unchecked"})
    @SuppressWarnings("resource")
    public static void main(String[] args) throws FileNotFoundException {


//        CsvToBean csv = new CsvToBean();
//        String csvFilename = "src/csv_data_sources/data.csv";
//        CSVReader csvReader = new CSVReader(new FileReader(csvFilename));
//        //Set column mapping strategy
//        List list = csv.parse(setColumMapping(), csvReader);
//        for (Object object : list) {
//            Employee employee = (Employee) object;
//            System.out.println(employee);
//        }

        //Build reader instance
        //Read data.csv
        //Default seperator is comma
        //Default quote character is double quote

        //Start reading from line number 2 (line numbers start from zero)

        try {


            Connection connection = DriverManager.getConnection ("jdbc:mysql://localhost:3306/mydb?" +
                    "user=root&password=&serverTimezone=UTC");
            connection.setAutoCommit (false);

            Statement statementDel = connection.createStatement ();
            statementDel.executeUpdate ("DELETE FROM Department");

            String sql = "insert into Department (MANAGER_ID, " +
                    "DEPARTMENT_NAME," +
                    "MGR_FIRST_NAME, " +
                    "DEPARTMENT_ID, " +
                    "MGR_LAST_NAME) " +
                    "values(?,?,?,?,?)";

            PreparedStatement statement = connection.prepareStatement (sql);
            BufferedReader lineReader = new BufferedReader (new FileReader ("src/csv_data_sources/Departments.csv"));


            consumeDepartmentCSVandSetParam (statement, lineReader);

            Statement statementDelEm = connection.createStatement ();
            statementDelEm.executeUpdate ("DELETE FROM employee");

            sql = "INSERT INTO employee (FIRST_NAME, LAST_NAME, EMAIL, HIRE_DATE, SALARY, DEPARTMENT_ID, MANAGER_ID, JOB_TITLE, MIN_SALARY, MAX_SALARY, EMPLOYEE_ID, COMMISSION_PCT) values(?,?,?,?,?,?,?,?,?,?,?,?)";
//            sql = "insert into Employee ( FIRST_NAME, " + "LAST_NAME," +
//                    "EMAIL, " +
//                    "HIRE_DATE, " +
//                    "SALARY " +
//                    "DEPARTMENT_ID" +
//                    "MANAGER_ID" +
//                    "JOB_TITLE" +
//                    "MIN_SALARY" +
//                    "MAX_SALARY" +
//                    "EMPLOYEE_ID" +
//                    "COMMISSION_PCT" +
//                    " values(?,?,?,?,?,?,?,?,?,?,?,?)";

            statement = connection.prepareStatement (sql);

            consumeEmployeeCSVandSetParam (statement, "src/csv_data_sources/Employees.csv");

            connection.commit ();
            connection.close ();
            System.out.println ("Data has been inserted successfully.");

        } catch (Exception exception) {
            exception.printStackTrace ();
        }
    }

//    @SuppressWarnings({"rawtypes", "unchecked"})
//    private static ColumnPositionMappingStrategy setColumMapping()
//    {
//        ColumnPositionMappingStrategy strategy = new ColumnPositionMappingStrategy();
//        strategy.setType(Employee.class);
//        String[] columns = new String[] {"id", "firstName", "lastName", "country", "age"};
//        strategy.setColumnMapping(columns);
//        return strategy;
//    }

    private static void consumeDepartmentCSVandSetParam(PreparedStatement statement, BufferedReader lineReader) throws IOException, SQLException {
        String lineText = null;
        lineReader.readLine ();
        while ((lineText = lineReader.readLine ()) != null) {
            String[] data = lineText.split (",");
            String MANAGER_ID = data[0];
            String DEPARTMENT_NAME = data[1];
            String MGR_FIRST_NAME = data[2];
            String DEPARTMENT_ID = data[3];
            String MGR_LAST_NAME = data[4];
            statement.setInt (1, parseInt (MANAGER_ID));
            statement.setString (2, DEPARTMENT_NAME);
            statement.setString (3, MGR_FIRST_NAME);
            statement.setInt (4, parseInt (DEPARTMENT_ID));
            statement.setString (5, MGR_LAST_NAME);
            statement.addBatch ();
        }
        lineReader.close ();
        statement.executeBatch ();
    }

    private static void consumeEmployeeCSVandSetParam(PreparedStatement statement, String filePath) throws IOException, SQLException, ParseException {

        try (CSVReader reader = new CSVReader (new FileReader (filePath),
                ',', '"', 0)) {
            String[] nextLine;
            String[] headerRow = nextLine = reader.readNext ();
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
//                    SimpleDateFormat sd1 = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
//                    Date dt = sd1.parse (HIRE_DATE);
//                    SimpleDateFormat sd2 = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
//                    String newDate = sd2.format (dt);
//                    System.out.println (newDate);
                    statement.setString (4, HIRE_DATE);
                    if (SALARY.length () != 0)
                        statement.setDouble (5, Double.parseDouble (SALARY));
                    else
                        statement.setNull (5, Types.DOUBLE);

                    if (DEPARTMENT_ID.length () != 0)
                        statement.setInt (6, parseInt (DEPARTMENT_ID));
                    else
                        statement.setInt (6, 0);

                    if (MANAGER_ID.length () != 0)
                        statement.setInt (7, parseInt (MANAGER_ID));
                    else
                        statement.setInt (7, 0);

                    statement.setString (8, JOB_TITLE);
                    statement.setDouble (9, parseDouble (MIN_SALARY));
                    statement.setDouble (10, parseDouble (MAX_SALARY));
                    statement.setInt (11, parseInt (EMPLOYEE_ID));
                    if (COMMISSION_PCT.length () != 0)
                        statement.setDouble (12, Double.parseDouble (COMMISSION_PCT));
                    else
                        statement.setDouble (12, 0.0);

                    statement.addBatch ();
                    statement.executeBatch ();
                }
            }
        } catch (IOException e) {
            e.printStackTrace ();
        }

    }

    public static boolean isParsable(String input) {
        try {
            Integer.parseInt (input);
            return true;
        } catch (final NumberFormatException e) {
            return false;
        }
    }
}
