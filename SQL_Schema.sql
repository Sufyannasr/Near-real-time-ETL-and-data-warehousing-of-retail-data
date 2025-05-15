CREATE DATABASE Datawarehouse;

USE Datawarehouse;

CREATE TABLE Time (
    Order_ID int primary key,
    Time_ID INT not null,
    Order_Date varchar(400)
);



CREATE TABLE Product (
    Product_ID INT PRIMARY KEY,
    Product_Name VARCHAR(255) NOT NULL,
    Product_Price double,
    Quantity INT 
);

-- Create the "Customer" table
CREATE TABLE Customer (
    Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(255) NOT NULL,
    Gender VARCHAR(50) NOT NULL
);


CREATE TABLE Supplier (
    Supplier_ID INT PRIMARY KEY,
    Supplier_Name VARCHAR(255) NOT NULL
);


CREATE TABLE Store (
    Store_ID INT PRIMARY KEY,
    Store_Name VARCHAR(255) NOT NULL
);


CREATE TABLE MetroSales (
    Order_ID INT NOT NULL,
    Customer_ID INT NOT NULL,
    Product_ID INT NOT NULL,
    Store_ID int not null,
    Supplier_ID int not null,
    Sales double ,
    PRIMARY KEY (Order_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES Supplier(Supplier_ID),
	FOREIGN KEY (Store_ID) REFERENCES Store(Store_ID),
    FOREIGN KEY (Customer_ID) REFERENCES Customer(Customer_ID),
    FOREIGN KEY (Product_ID) REFERENCES Product(Product_ID)
);


START TRANSACTION;

INSERT IGNORE INTO datawarehouse.product (Product_ID, Product_Name, Product_Price, Quantity)
SELECT DISTINCT product_id, product_name, product_price, quantity
FROM project.datawarehouse;

COMMIT;

START TRANSACTION;

INSERT IGNORE INTO datawarehouse.customer (Customer_ID, Customer_Name,Gender)
SELECT DISTINCT customer_id,customer_name,gender
FROM project.datawarehouse;


COMMIT;

START TRANSACTION;

INSERT IGNORE INTO datawarehouse.supplier (Supplier_ID, Supplier_Name)
SELECT DISTINCT supplier_id,supplier_name
FROM project.datawarehouse;


COMMIT;

START TRANSACTION;

INSERT IGNORE INTO datawarehouse.store (Store_ID, Store_Name)
SELECT DISTINCT store_id,store_name
FROM project.datawarehouse;


COMMIT;

START TRANSACTION;

INSERT IGNORE INTO datawarehouse.time (Order_ID, Time_ID, Order_Date)
SELECT DISTINCT order_id,time_id,order_date
FROM project.datawarehouse;


COMMIT;

START TRANSACTION;

INSERT IGNORE INTO datawarehouse.metrosales (Order_ID, Customer_ID, Product_ID,Store_ID,Supplier_ID,Sales)
SELECT DISTINCT order_id,customer_id,product_id,store_id,supplier_id,total_sale
FROM project.datawarehouse;


COMMIT;


