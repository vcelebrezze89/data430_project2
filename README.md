DATA-430 Project 2
Airflow + SPark ETL Pipeline

Overview:
    
    This project demonstrates an end-to-end ETL pipeline using Apache Airflow, Apache Spark, Docker, and PostgreSQL. The goal of the project is to simulate a real-world data engineering workflow that generates synthetic data, processes it using distributed computing, and produces analytics outputs.

    The pipeline generates large datasets using Python and Faker, validates the data, processes the data using Apache Spark, and produces analytics results. Apache Airflow is used to orchestrate and manage the entire workflow, while Docker is used to run all services in a consistent environment.

    This project builds on Project 1, where Apache NiFi was used to create an ETL pipeline. In Project 2, the pipeline is expanded using Apache Airflow and Apache Spark to create a more scalable and production-style architecture.

Tech Used:

    Apache Airflow (Workflow orchestration)
    Apache Spark (Distributed data processing)
    Docker (Containerized environment)
    PostgreSQL (Database storage)
    Python (Data generation and processing)
    Faker (Synthetic data generation)

    These technologies work together to create a scalable and reproducible ETL pipeline.

Project Architecture:

    The pipeline follows this workflow: Data Generation -> Validation -> Spark Cleansing -> Spark Analytics -> Output

    Pipeline Steps
       -Generate synthetic datasets using Faker
       -Validate that files exist and are ready for processing
       -Clean and transform datasets using Apache Spark
       -Run analytics using Spark
       -Store processed outputs
    
    The pipeline is orchestrated using Apache Airflow and runs inside Docker containers.

Dataset Description:

    Four datasets are generated:

    Customers
        Contains customer information including:
            Customer ID
            Name
            Email
            Phone
            Address
            Date of Birth
            Registration Date
    
    Suppliers
        Contains supplier information including:
            Supplier ID
            Company Name
            Contact Information
            Industry
            Rating
    
    Products
        Contains product details including:
            Product ID
            Product Name
            Category
            Price
            Cost
            Supplier ID
    
    Daily Sales
        Contains transaction data including:
            Sale ID
            Customer ID
            Product ID
            Quantity
            Total Amount
            Payment Method
    
    Each dataset contains 100,000 records to simulate large-scale data processing.

