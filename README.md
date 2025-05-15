# Near-real-time-ETL-and-data-warehousing-of-retail-data
This project aimed to implement the ETL process of a data warehouse, subsequently 
performing OLAP queries against the data to simulate the real-world process. A mesh join 
algorithm was utilized in java to create a large streaming tuple which involved the disk buffer 
(transactions) and the master data (customer and products data), which was then loaded into a 
star schema in a MySQL server for analyzing. 
Schema of the data warehouse 
For the representation of the data warehouse in a MySQL environment, a star schema was 
implemented which had the following components: 
 Metro Sales (Fact table): The fact table which served as the subject for the data 
warehouse, it references all the primary keys of the dimension tables for querying the 
data. 
 Time (Dimension): Dimension table for storing all the data relevant to the dates and 
time of the orders being placed. 
 Customer (Dimension): The customer dimension stores information regarding the 
customers who buy the products. 
 Product (Dimension): This table holds all data concerning the products being purchased 
and their attributes. 
 Supplier (Dimension): The supplier table stores the supplier’s name with respect to 
their ID. 
 Store (Dimension): This dimension holds information concerning the name of the store 
and the number used for its identification. 
Mesh join algorithm 
The mesh join algorithm is used to join streaming data with a large dataset that is already 
present in the hard disk. This join is performed in real time, and the joint dataset is typically 
used for further streaming. The algorithm works by initially breaking the stored dataset into 
blocks which are then incrementally loaded into memory, at any given moment, only a portion 
of this dataset is present in memory. The streaming data is then matched against the stored 
data, which is systematically updated.  
By minimizing disk I/O operations, mesh join achieves improved efficiency. Every incoming 
streaming tuple is loaded and processed in small, manageable chunks rather than the entire 
dataset. This allows the algorithm to scale well with large datasets while still maintaining 
responsiveness to the streaming data. As a result, mesh join can handle real-time or near-real
time processing scenarios, such as real-time analytics or sensor data processing, where timely 
insights are critical.
