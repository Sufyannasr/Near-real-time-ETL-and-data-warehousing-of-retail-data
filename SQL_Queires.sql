#Q1
SELECT 
    MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) AS Month,
    p.Product_ID,
    p.Product_Name,
    SUM(CASE 
            WHEN DAYOFWEEK(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) IN (1, 7) THEN ms.Sales  
            ELSE 0
        END) AS Weekend_Sales,
    SUM(CASE 
            WHEN DAYOFWEEK(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) NOT IN (1, 7) THEN ms.Sales  
            ELSE 0
        END) AS Weekday_Sales,
    SUM(ms.Sales) AS Total_Sales
FROM 
    datawarehouse.metrosales ms
JOIN 
    datawarehouse.product p ON ms.Product_ID = p.Product_ID
JOIN 
    datawarehouse.time t ON ms.Order_ID = t.Order_ID
GROUP BY 
    MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')),
    p.Product_ID,
    p.Product_Name
ORDER BY 
    Total_Sales DESC
LIMIT 5;

#Q2
WITH QuarterlyRevenue AS (
    SELECT 
        s.Store_ID,
        s.Store_Name,
        QUARTER(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) AS Quarter,
        SUM(ms.Sales) AS Total_Revenue
    FROM 
        datawarehouse.metrosales ms
    JOIN 
        datawarehouse.time t ON ms.Order_ID = t.Order_ID
    JOIN 
        datawarehouse.store s ON ms.Store_ID = s.Store_ID
    WHERE 
        YEAR(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) = 2019  
    GROUP BY 
        s.Store_ID, s.Store_Name, QUARTER(STR_TO_DATE(t.Order_Date, '%Y-%m-%d'))
),
RevenueGrowth AS (
    SELECT 
        r1.Store_ID,
        r1.Store_Name,
        r1.Quarter,
        r1.Total_Revenue AS Current_Quarter_Revenue,
        r2.Total_Revenue AS Previous_Quarter_Revenue,
        IFNULL(
            ((r1.Total_Revenue - r2.Total_Revenue) / r2.Total_Revenue) * 100,
            0
        ) AS Growth_Rate
    FROM 
        QuarterlyRevenue r1
    LEFT JOIN 
        QuarterlyRevenue r2 ON r1.Store_ID = r2.Store_ID 
                               AND r1.Quarter = r2.Quarter + 1  
    WHERE 
        r1.Quarter > 1  
)
SELECT 
    Store_ID,
    Store_Name,
    Quarter,
    Current_Quarter_Revenue,
    Previous_Quarter_Revenue,
    Growth_Rate
FROM 
    RevenueGrowth
ORDER BY 
    Store_ID, Quarter;

#Q3
SELECT 
    st.Store_ID,
    st.Store_Name,
    su.Supplier_ID,
    su.Supplier_Name,
    p.Product_ID,
    p.Product_Name,
    SUM(ms.Sales) AS Total_Sales_Contribution
FROM 
    datawarehouse.metrosales ms
JOIN 
    datawarehouse.product p ON ms.Product_ID = p.Product_ID
JOIN 
    datawarehouse.supplier su ON ms.Supplier_ID = su.Supplier_ID
JOIN 
    datawarehouse.store st ON ms.Store_ID = st.Store_ID
GROUP BY 
    st.Store_ID, 
    st.Store_Name, 
    su.Supplier_ID, 
    su.Supplier_Name, 
    p.Product_ID, 
    p.Product_Name
ORDER BY 
    st.Store_Name, 
    su.Supplier_Name, 
    p.Product_Name;

#Q4
SELECT 
    p.Product_ID,
    p.Product_Name,
    CASE 
        WHEN MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) IN (9, 10, 11) THEN 'Fall'
        WHEN MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) IN (12, 1, 2) THEN 'Winter'
    END AS Season,
    SUM(ms.Sales) AS Total_Sales
FROM 
    datawarehouse.metrosales ms
JOIN 
    datawarehouse.product p ON ms.Product_ID = p.Product_ID
JOIN 
    datawarehouse.time t ON ms.Order_ID = t.Order_ID
GROUP BY 
    p.Product_ID, 
    p.Product_Name, 
    Season
ORDER BY 
    p.Product_Name, 
    FIELD(Season, 'Spring', 'Summer', 'Fall', 'Winter');

#Q5
WITH MonthlySales AS (
    SELECT 
        ms.Store_ID AS StoreID,
        ms.Supplier_ID AS SupplierID,
        YEAR(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) AS SaleYear,
        MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) AS SaleMonth,
        SUM(ms.Sales) AS MonthlyRevenue
    FROM 
        datawarehouse.metrosales ms
    JOIN 
        datawarehouse.time t ON ms.Order_ID = t.Order_ID
    WHERE 
        YEAR(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) = 2019 
    GROUP BY 
        ms.Store_ID, ms.Supplier_ID, YEAR(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')), MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d'))
),
RevenueVolatility AS (
    SELECT 
        current.StoreID,
        current.SupplierID,
        current.SaleYear,
        current.SaleMonth,
        current.MonthlyRevenue AS CurrentMonthRevenue,
        previous.MonthlyRevenue AS PreviousMonthRevenue,
        IFNULL(
            ((current.MonthlyRevenue - previous.MonthlyRevenue) / previous.MonthlyRevenue) * 100,
            0
        ) AS RevenueVolatility
    FROM 
        MonthlySales current
    LEFT JOIN 
        MonthlySales previous 
        ON current.StoreID = previous.StoreID 
        AND current.SupplierID = previous.SupplierID 
        AND (current.SaleYear = previous.SaleYear AND current.SaleMonth = previous.SaleMonth + 1)
)
SELECT 
    StoreID,
    SupplierID,
    SaleYear,
    SaleMonth,
    CurrentMonthRevenue,
    PreviousMonthRevenue,
    RevenueVolatility
FROM 
    RevenueVolatility
WHERE 
    SaleYear = 2019 
ORDER BY 
    StoreID, 
    SupplierID, 
    SaleYear, 
    SaleMonth;

#Q6
WITH ProductPairs AS (
    SELECT 
        p1.Product_ID AS Product_A,
        p2.Product_ID AS Product_B,
        COUNT(*) AS Pair_Count
    FROM 
        datawarehouse.metrosales p1
    JOIN 
        datawarehouse.metrosales p2 
        ON p1.Order_ID = p2.Order_ID 
        AND p1.Product_ID != p2.Product_ID 
    GROUP BY 
        p1.Product_ID, p2.Product_ID
),
RankedPairs AS (
    SELECT 
        Product_A,
        Product_B,
        Pair_Count,
        DENSE_RANK() OVER (ORDER BY Pair_Count DESC) AS Pair_Rank
    FROM 
        ProductPairs
)
SELECT 
    rp.Product_A AS Product_A_ID,
    pA.Product_Name AS Product_A_Name,
    rp.Product_B AS Product_B_ID,
    pB.Product_Name AS Product_B_Name,
    rp.Pair_Count
FROM 
    RankedPairs rp
JOIN 
    datawarehouse.product pA ON rp.Product_A = pA.Product_ID
JOIN 
    datawarehouse.product pB ON rp.Product_B = pB.Product_ID
WHERE 
    rp.Pair_Rank <= 5 
ORDER BY 
    rp.Pair_Count DESC;

#Q7
SELECT 
    ms.Store_ID,
    ms.Supplier_ID,
    p.Product_ID,
    YEAR(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) AS Year,
    SUM(ms.Sales) AS Total_Revenue
FROM 
    datawarehouse.metrosales ms
JOIN 
    datawarehouse.product p ON ms.Product_ID = p.Product_ID
JOIN 
    datawarehouse.time t ON ms.Order_ID = t.Order_ID
GROUP BY 
    ROLLUP(ms.Store_ID, ms.Supplier_ID, p.Product_ID, YEAR(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')))
ORDER BY 
    ms.Store_ID, ms.Supplier_ID, p.Product_ID, Year;

#Q8
SELECT 
    p.Product_ID,
    p.Product_Name,
    YEAR(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) AS Year,
    SUM(CASE 
            WHEN MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) BETWEEN 1 AND 6 
            THEN ms.Sales 
            ELSE 0 
        END) AS Revenue_First_Half,
    SUM(CASE 
            WHEN MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) BETWEEN 7 AND 12 
            THEN ms.Sales 
            ELSE 0 
        END) AS Revenue_Second_Half,
    SUM(ms.Sales) AS Revenue_Total,
    SUM(CASE 
            WHEN MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) BETWEEN 1 AND 6 
            THEN p.Quantity 
            ELSE 0 
        END) AS Quantity_First_Half,
    SUM(CASE 
            WHEN MONTH(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) BETWEEN 7 AND 12 
            THEN p.Quantity 
            ELSE 0 
        END) AS Quantity_Second_Half,
    SUM(p.Quantity) AS Quantity_Total
FROM 
    datawarehouse.metrosales ms
JOIN 
    datawarehouse.product p ON ms.Product_ID = p.Product_ID
JOIN 
    datawarehouse.time t ON ms.Order_ID = t.Order_ID
GROUP BY 
    p.Product_ID, p.Product_Name, YEAR(STR_TO_DATE(t.Order_Date, '%Y-%m-%d'))
ORDER BY 
    p.Product_ID, Year
LIMIT 0, 50000;

#Q9
WITH DailySales AS (
    SELECT 
        ms.Product_ID,
        p.Product_Name,
        t.Order_Date,
        SUM(ms.Sales) AS Daily_Sales
    FROM 
        datawarehouse.metrosales ms
    JOIN 
        datawarehouse.product p ON ms.Product_ID = p.Product_ID
    JOIN 
        datawarehouse.time t ON ms.Order_ID = t.Order_ID
    GROUP BY 
        ms.Product_ID, p.Product_Name, t.Order_Date
),
AverageSales AS (
    SELECT 
        Product_ID,
        Product_Name,
        AVG(Daily_Sales) AS Avg_Daily_Sales
    FROM 
        DailySales
    GROUP BY 
        Product_ID, Product_Name
)
SELECT 
    ds.Product_ID,
    ds.Product_Name,
    ds.Order_Date,
    ds.Daily_Sales,
    avg_sales.Avg_Daily_Sales,
    CASE 
        WHEN ds.Daily_Sales > 2 * avg_sales.Avg_Daily_Sales THEN 'Outlier'
        ELSE 'Normal'
    END AS Sales_Flag
FROM 
    DailySales ds
JOIN 
    AverageSales avg_sales ON ds.Product_ID = avg_sales.Product_ID
ORDER BY 
    ds.Product_ID, ds.Order_Date;

#Q10
CREATE VIEW datawarehouse.STORE_QUARTERLY_SALES AS 
SELECT      
    s.Store_ID,
    s.Store_Name,
    YEAR(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) AS Sales_Year,
    QUARTER(STR_TO_DATE(t.Order_Date, '%Y-%m-%d')) AS Sales_Quarter,
    SUM(ms.Sales) AS Total_Quarterly_Sales
FROM      
    datawarehouse.metrosales ms
JOIN      
    datawarehouse.store s ON ms.Store_ID = s.Store_ID
JOIN      
    datawarehouse.time t ON ms.Order_ID = t.Order_ID
GROUP BY      
    s.Store_ID, s.Store_Name, Sales_Year, Sales_Quarter
ORDER BY      
    s.Store_Name, Sales_Year, Sales_Quarter;

