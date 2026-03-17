# PySpark Assignment

A structured PySpark project covering DataFrames, transformations, joins,
UDFs, file formats, and managed/external tables.

---

## Project Structure
```
pyspark-assignment/
├── src/
│   ├── question_1/          # Purchase & Product DataFrames
│   │   ├── sub_qn_1/        # Create DataFrames
│   │   ├── sub_qn_2/        # Customers who bought only iphone13
│   │   ├── sub_qn_3/        # Customers who upgraded to iphone14
│   │   └── sub_qn_4/        # Customers who bought all models
│   │
│   ├── question_2/          # Credit Card DataFrame
│   │   ├── sub_qn_1/        # Create DataFrame with different read methods
│   │   ├── sub_qn_2/        # Print number of partitions
│   │   ├── sub_qn_3/        # Increase partitions to 5
│   │   ├── sub_qn_4/        # Decrease partitions back to original
│   │   ├── sub_qn_5/        # UDF to mask card number
│   │   └── sub_qn_6/        # Final output with masked card number
│   │
│   ├── question_3/          # User Activity Log
│   │   ├── sub_qn_1/        # Create DataFrame with custom schema
│   │   ├── sub_qn_2/        # Rename columns dynamically
│   │   ├── sub_qn_3/        # Actions per user in last 7 days
│   │   ├── sub_qn_4/        # Convert timestamp to login_date
│   │   ├── sub_qn_5/        # Write CSV with different options
│   │   └── sub_qn_6/        # Write managed table
│   │
│   ├── question_4/          # JSON Employee Data
│   │   ├── sub_qn_1/        # Read JSON dynamically
│   │   ├── sub_qn_2/        # Flatten nested DataFrame
│   │   ├── sub_qn_3/        # Count comparison (flattened vs not)
│   │   ├── sub_qn_4/        # explode vs explode_outer vs posexplode
│   │   ├── sub_qn_5/        # Filter id == 0001
│   │   ├── sub_qn_6/        # Rename camelCase to snake_case
│   │   ├── sub_qn_7/        # Add load_date column
│   │   ├── sub_qn_8/        # Add year, month, day columns
│   │   └── sub_qn_9/        # Write partitioned JSON table
│   │
│   └── question_5/          # Employee, Department, Country
│       ├── sub_qn_1/        # Create all 3 DataFrames
│       ├── sub_qn_2/        # Average salary by department
│       ├── sub_qn_3/        # Employees whose name starts with 'm'
│       ├── sub_qn_4/        # Add bonus column (salary * 2)
│       ├── sub_qn_5/        # Reorder columns
│       ├── sub_qn_6/        # Inner, left, right joins
│       ├── sub_qn_7/        # Replace state code with country name
│       ├── sub_qn_8/        # Lowercase columns + add load_date
│       └── sub_qn_9/        # Write external CSV and Parquet tables
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Prerequisites

- Python 3.8+
- Java 8 or Java 11
- PySpark 3.5.0

---

## Setup

### Step 1 - Clone the repository
```bash
git clone https://github.com/swatikonnuri3/pyspark-assignment.git
cd pyspark-assignment
```

### Step 2 - Create and activate virtual environment
```powershell
python -m venv .venv
.venv\Scripts\activate
```

### Step 3 - Install dependencies
```powershell
pip install -r requirements.txt
```

### Step 4 - Verify Java is installed
```powershell
java -version
```

---

## Running a Driver
```powershell
# Activate venv first
.venv\Scripts\activate

# Navigate to the sub-question folder
cd src\question_1\sub_qn_1

# Run the driver
python driver.py
```

---

## Running Tests
```powershell
# Run tests for a specific sub-question
cd src\question_1\sub_qn_2
pytest test_cases.py -v

# Run all tests for a question
cd src\question_1
pytest -v

# Run all tests in the project
cd C:\Users\Swati\PycharmProjects\pyspark-assignment
pytest src\ -v
```


## Questions Overview

| Question | Topic                        | Sub-questions |
|----------|------------------------------|---------------|
| Q1       | Purchase & Product Data      | 4             |
| Q2       | Credit Card Masking          | 6             |
| Q3       | User Activity Log            | 6             |
| Q4       | Nested JSON Processing       | 9             |
| Q5       | Employee Data & Joins        | 9             |