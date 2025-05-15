package u24;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Scanner; 

public class Abcd {

    public static void main(String[] args) {
        //Defining the user details
        String link = "jdbc:mysql://localhost:3308/project";  
        Scanner scanner = new Scanner(System.in);

        //Asking the user for information regarding the datawarehouse connection
        System.out.print("Enter the username: ");

        String name = scanner.nextLine();

        Scanner scanner2 = new Scanner(System.in);

        System.out.print("Enter the password: ");
   
        String password = scanner2.nextLine();

        // Close the scanner
        scanner.close();
        scanner2.close();
        Connection con = null;
        Statement stmt = null;
        PreparedStatement pstmt = null; 

        //Making a hashtable for the data
        HashMap<Integer, TransactionTuple> transactionHashTable = new HashMap<>();
        Queue<Integer> transactionQueue = new LinkedList<>();

        int partition = 10; 

        try {
            //Connecting to the mysql server
            Class.forName("com.mysql.cj.jdbc.Driver");

            con = DriverManager.getConnection(link, name, password);
            System.out.println("The connection to the mySQL server has been established");

            stmt = con.createStatement();

            String insertSQL = "INSERT INTO datawarehouse2 (order_id, order_date, product_name, product_price, product_id, quantity, " +
                               "customer_id, customer_name, gender, supplier_id, supplier_name, store_id, store_name, time_id, total_sale) " +
                               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            pstmt = con.prepareStatement(insertSQL);

            //Receiving the master data
            Map<Integer, CustomerTuple> customersMap = loadCustomersTable("customers_data", stmt);
            Map<Integer, ProductTuple> productsMap = loadProductsTable("products_data", stmt);

            //Collecting the streaming data from the csv file
            System.out.println("\nProcessing transactions from CSV file...");
            String transactionsFilePath = "C:\\Users\\Legion\\Downloads/transactions.csv"; // Replace with your CSV file path
            List<TransactionTuple> transactions = loadTransactionsFromCSV(transactionsFilePath);

            //Applying mesh join algorithm
            for (TransactionTuple transaction : transactions) {
                transactionHashTable.put(transaction.getOrderId(), transaction);
                transactionQueue.add(transaction.getOrderId());
                if (transactionQueue.size() >= partition) {
                    for (Map.Entry<Integer, CustomerTuple> customerEntry : customersMap.entrySet()) {
                        CustomerTuple customer = customerEntry.getValue();
                        for (Map.Entry<Integer, ProductTuple> productEntry : productsMap.entrySet()) {
                            ProductTuple product = productEntry.getValue();
                            for (Integer orderId : transactionQueue) {
                                TransactionTuple streamTuple = transactionHashTable.get(orderId);
                                if (streamTuple.getCustomerId() == customer.getCustomerId() &&
                                        streamTuple.getProductId() == product.getProductId()) {
                                    double totalSale = streamTuple.getQuantity() * product.getProductPrice();
                                    streamTuple.setCustomerName(customer.getCustomerName());
                                    streamTuple.setGender(customer.getGender());
                                    streamTuple.setProductName(product.getProductName());
                                    streamTuple.setSupplierName(product.getSupplierName());
                                    streamTuple.setStoreName(product.getStoreName());
                                    streamTuple.setTotalSale(totalSale);
                                    streamTuple.setSupplierId(product.getSupplierId());
                                    streamTuple.setStoreId(product.getStoreId());
                                    streamTuple.setProductPrice(product.getProductPrice());
                                    System.out.println("The mesh join was succesfull");
                                    System.out.println("Order ID: " + streamTuple.getOrderId() +
                                            ", Order Date: " + streamTuple.getOrderDate() +
                                            ", Product Name: " + streamTuple.getProductName() +
                                            ", Total Sale: " + streamTuple.getTotalSale());

                                    //Loading the data in the data ware house 
                                    pstmt.setInt(1, streamTuple.getOrderId());
                                    pstmt.setString(2, streamTuple.getOrderDate());
                                    pstmt.setString(3, streamTuple.getProductName());
                                    pstmt.setDouble(4, streamTuple.getProductPrice());
                                    pstmt.setInt(5, streamTuple.getProductId());
                                    pstmt.setInt(6, streamTuple.getQuantity());
                                    pstmt.setInt(7, streamTuple.getCustomerId());
                                    pstmt.setString(8, streamTuple.getCustomerName());
                                    pstmt.setString(9, streamTuple.getGender());
                                    pstmt.setInt(10, streamTuple.getSupplierId());
                                    pstmt.setString(11, streamTuple.getSupplierName());
                                    pstmt.setInt(12, streamTuple.getStoreId());
                                    pstmt.setString(13, streamTuple.getStoreName());
                                    pstmt.setString(14, streamTuple.getTimeId());
                                    pstmt.setDouble(15, streamTuple.getTotalSale());

                                    pstmt.executeUpdate();
                                }
                            }
                        }
                    }
                    transactionQueue.clear();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) pstmt.close();
                if (stmt != null) stmt.close();
                if (con != null) con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    private static List<TransactionTuple> loadTransactionsFromCSV(String filePath) throws IOException {
        List<TransactionTuple> transactions = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                TransactionTuple transaction = new TransactionTuple(
                        Integer.parseInt(values[0]), 
                        values[1],                  
                        Integer.parseInt(values[2]), 
                        Integer.parseInt(values[3]), 
                        Integer.parseInt(values[4]), 
                        values[5]                   
                );
                transactions.add(transaction);
            }
        }
        return transactions;
    }

    private static Map<Integer, CustomerTuple> loadCustomersTable(String tableName, Statement stmt) throws SQLException {
        Map<Integer, CustomerTuple> customersMap = new HashMap<>();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);

        while (rs.next()) {
            CustomerTuple customer = new CustomerTuple(
                    rs.getInt("customer_id"),
                    rs.getString("customer_name"),
                    rs.getString("gender")  // Adding gender to the customer tuple
            );
            customersMap.put(customer.getCustomerId(), customer);
        }

        return customersMap;
    }

    private static Map<Integer, ProductTuple> loadProductsTable(String tableName, Statement stmt) throws SQLException {
        Map<Integer, ProductTuple> productsMap = new HashMap<>();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);

        while (rs.next()) {
            ProductTuple product = new ProductTuple(
                    rs.getInt("productID"),
                    rs.getString("productName"),
                    rs.getDouble("productPrice"),
                    rs.getInt("supplierID"), 
                    rs.getString("supplierName"), 
                    rs.getInt("storeID"),
                    rs.getString("storeName") 
            );
            productsMap.put(product.getProductId(), product);
        }

        return productsMap;
    }
}

abstract class MDTableTuple {
    public abstract int getId();
}

//Defining all the tuples
class CustomerTuple extends MDTableTuple {
    private int customerId;
    private String customerName;
    private String gender;

    public CustomerTuple(int customerId, String customerName, String gender) {
        this.customerId = customerId;
        this.customerName = customerName;
        this.gender = gender;
    }

    public int getCustomerId() {
        return customerId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getGender() {
        return gender;
    }

    @Override
    public int getId() {
        return customerId;
    }
}

class ProductTuple extends MDTableTuple {
    private int productId;
    private String productName;
    private double productPrice;
    private int supplierId;
    private String supplierName;
    private int storeId;
    private String storeName;

    public ProductTuple(int productId, String productName, double productPrice, int supplierId, 
                        String supplierName, int storeId, String storeName) {
        this.productId = productId;
        this.productName = productName;
        this.productPrice = productPrice;
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.storeId = storeId;
        this.storeName = storeName;
    }

    public int getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public double getProductPrice() {
        return productPrice;
    }

    public int getSupplierId() {
        return supplierId;
    }

    public String getSupplierName() {
        return supplierName;
    }

    public int getStoreId() {
        return storeId;
    }

    public String getStoreName() {
        return storeName;
    }

    @Override
    public int getId() {
        return productId;
    }
}

class TransactionTuple {
    private int orderId;
    private String orderDate;
    private int productId;
    private int quantity;
    private int customerId;
    private String timeId;
    private String customerName;
    private String productName;
    private String gender;
    private String supplierName;
    private int supplierId;
    private String storeName;
    private int storeId;
    private double productPrice;
    private double totalSale;

    public TransactionTuple(int orderId, String orderDate, int productId, int quantity, int customerId, String timeId) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.productId = productId;
        this.quantity = quantity;
        this.customerId = customerId;
        this.timeId = timeId;
    }

    public int getOrderId() {
        return orderId;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public int getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public int getCustomerId() {
        return customerId;
    }

    public String getTimeId() {
        return timeId;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setSupplierName(String supplierName) {
        this.supplierName = supplierName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public void setTotalSale(double totalSale) {
        this.totalSale = totalSale;
    }

    public void setSupplierId(int supplierId) {
        this.supplierId = supplierId;
    }

    public void setStoreId(int storeId) {
        this.storeId = storeId;
    }

    public void setProductPrice(double productPrice) {
        this.productPrice = productPrice;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getProductName() {
        return productName;
    }

    public String getGender() {
        return gender;
    }

    public String getSupplierName() {
        return supplierName;
    }

    public String getStoreName() {
        return storeName;
    }

    public int getSupplierId() {
        return supplierId;
    }

    public int getStoreId() {
        return storeId;
    }

    public double getProductPrice() {
        return productPrice;
    }

    public double getTotalSale() {
        return totalSale;
    }
}
