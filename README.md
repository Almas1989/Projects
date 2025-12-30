# 📊 Projects
## [Main page](https://github.com/Almas1989)

Welcome to my portfolio of data projects. This repository includes a selection of hands-on projects using SQL and Python for real-world data analysis tasks. These projects cover exploratory analysis, data cleaning, and correlation analysis—fundamental skills in the data analytics and engineering domains.

---

## 📁 Projects Overview

## 1. 🧮 [SQL vs Pandas — Salary Analytics Practice (DuckDB)](https://github.com/Almas1989/SQL_vs_pandas)

- [**Dataset**](https://github.com/Almas1989/SQL_vs_pandas/blob/main/salaries_clean.csv)
- [**EDA Notebook (SQL + Pandas)**](https://github.com/Almas1989/SQL_vs_pandas/blob/main/EDA(SQL,pandas).ipynb)
- [**SQL Practice 1**](https://github.com/Almas1989/SQL_vs_pandas/blob/main/SQL_pract_1.ipynb)
- [**SQL Practice 2 (Advanced)**](https://github.com/Almas1989/SQL_vs_pandas/blob/main/SQL_pract_2.ipynb)

- **Description**:  
  Interactive Jupyter notebooks for practicing SQL analytics using DuckDB on IT salary data from Kazakhstan. The project demonstrates a wide range of SQL patterns and techniques:
  - Data loading with `read_csv_auto` into in-memory DuckDB tables
  - Filtering & subqueries: `WHERE`, `EXISTS`, `IN`, `BETWEEN`, `ANY`, `ALL`
  - Aggregations: `AVG`, `SUM`, `COUNT`, `MIN`, `MAX`, `MEDIAN`, `ARRAY_AGG`
  - Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`
  - Named windows (`WINDOW` clause) and window filtering with `QUALIFY`
  - Hierarchical grouping: `ROLLUP`, `CUBE`, `GROUPING SETS`
  - Pivoting: `PIVOT` and manual `CASE`-based pivots
  - Time-series analysis: `DATE_TRUNC`, `EXTRACT`, rolling 90-day averages with `RANGE BETWEEN INTERVAL`
  - CTEs for cleaner query structure

  Typical analyses include: Top-N salaries per city/profession, grade-level comparisons, percentile rankings, period-over-period changes, and multi-level hierarchical summaries.

- **Skills Practiced**: `SQL analytics, window functions, QUALIFY, named windows, ROLLUP/CUBE, pivoting, time-series analysis, DuckDB, CTEs, subqueries`

- **STACK**: Python, DuckDB, Pandas, NumPy, Matplotlib, Seaborn, Jupyter Notebook

---

## 2. 🔍 [Data cleaning and Exploratory Data Analysis (MySQL)](https://github.com/Almas1989/Projects/tree/main/EDA_MySQL)

[Dataset](https://github.com/Almas1989/Projects/blob/main/EDA_MySQL/layoffs.csv)

- **Description**:  
  This project demonstrates exploratory data analysis using SQL on a structured dataset. It includes operations such as:
  - Handling and replacing `NULL` values, Standardizing formats (e.g., date and text case), Removing duplicates, Creating temporary clean views and applying transformation logic, Renaming columns for clarity and better readability
  - Identifying trends through grouping and filtering
  - Calculating statistical aggregates like averages, counts, and percentages
  - Using `CASE` statements to classify data
  - Applying conditional filtering to uncover insights

  The goal was to simulate a typical data exploration workflow within a relational database before building dashboards or models.

- **Skills Practiced : `Data cleaning, SQL scripting, formatting, NULL handling, view creation, aggregations, conditional logic, pattern recognition, CTE and window functions`**

- **STACK**: MySQL (PostgreSQL-compatible)

---

## 3. 🎬 [Movie Correlation Project (Python)](https://github.com/Almas1989/movie_corel)

- [**Dataset**](https://github.com/Almas1989/movie_corel/blob/main/movies.csv)
- [**REPORT**](https://github.com/Almas1989/movie_corel/blob/main/report.md)
- [**Jupyter notebook with code**](https://github.com/Almas1989/movie_corel/blob/main/movie_corell.ipynb)

- **Description**:  
  This Jupyter Notebook investigates how different variables (budget, gross revenue, runtime, etc.) relate to each other in a movie dataset. Steps included:
  - Data cleaning and preprocessing using `pandas`
  - Visualizing distributions and relationships with `plotly.express`
  - Calculating correlation and plotting a heatmap
  - Identifying which variables strongly affect a movie’s box office success

  The notebook helps answer business questions like: "Do bigger budgets lead to higher gross?" or "Which factors most influence revenue?"

- **Skills Practiced: `Exploratory data analysis, feature correlation, data visualization, Python scripting`**

- **STACK**: Python, Pandas, Plotly.express, Jupyter Notebook

![Correlation matrix](https://github.com/Almas1989/movie_corel/blob/main/10.png)

---

## 4. 🧪 [A/B Test – Marketing Campaign Analysis (Python)](https://www.kaggle.com/code/almasscorp/ab-test)

- **Description**:  
  This project evaluates the effectiveness of a new landing page through A/B testing. The analysis includes:
  - Comparing conversion rates between control (Group A) and treatment (Group B)
  - Conducting a z-test to assess statistical significance
  - Visualizing results and drawing business conclusions

  The goal was to simulate a real-world decision-making process using hypothesis testing.

- **Skills Practiced**: A/B testing, hypothesis testing, statistical analysis, data visualization

- **STACK**: Python, Pandas, SciPy, Seaborn, Matplotlib

---

## 5. 📈 [Marketing Cohort Analysis (Python)](https://github.com/Almas1989/marketing_cohort_analysis)

- [**Dataset**](https://github.com/Almas1989/marketing_cohort_analysis/tree/main/marketing_dataset)
- [**Jupyter notebook with code**](https://github.com/Almas1989/marketing_cohort_analysis/blob/main/marketing-insights-for-e-commerce-company.ipynb)

- **Description**:  
  This project analyzes the **retention** and **purchase behavior** of customers for an e-commerce company using **cohort analysis**. The notebook walks through:
  - Cleaning and transforming user transaction data with `pandas`
  - Assigning cohort groups based on user acquisition month
  - Calculating retention rates across cohorts
  - Visualizing cohort retention heatmaps with `seaborn` and `matplotlib`
  - Deriving insights about user engagement and drop-off patterns

  This type of analysis is commonly used by product teams and marketing analysts to understand customer lifecycle, evaluate loyalty, and improve retention strategies.

- **Skills Practiced: `Cohort analysis, retention analysis, data cleaning, data visualization, pandas transformations`**

- **STACK**: Python, Pandas, Matplotlib, Seaborn, Jupyter Notebook

---

## 6. 📂 [CSV Processor – Automated Data Cleaning (Python)](https://github.com/Almas1989/csv_processor)

- **Description**:  
  This project automates the processing of raw CSV files to prepare them for analysis. It includes:  
  - Reading input CSVs and handling encoding issues  
  - Cleaning data by removing empty rows and standardizing column formats  
  - Filtering and transforming specific fields (e.g., renaming, converting values)  
  - Saving the cleaned output to a new CSV file  
  
  The goal was to streamline repetitive preprocessing steps in data workflows.

- **Skills Practiced**: Data preprocessing, automation, file handling, basic ETL logic

- **STACK**: Python, Pandas, CSV module

---

## 7. 📚 [Wikipedia Parser – Async Scraper + AI Summarizer](https://github.com/Almas1989/wiki_parser)

- **Description**:  
  Asynchronous API that recursively scrapes Wikipedia articles (up to 5 levels deep) and generates AI-powered summaries using DeepSeek API. Parsed articles, their relationships, and summaries are stored in a PostgreSQL database.

- **Features**:  
  - Recursive Wikipedia parsing with parent-child relations  
  - AI-generated summaries for articles  
  - Async FastAPI backend with PostgreSQL  
  - Fully Dockerized for easy deployment  

- **Skills Practiced**:  
  Async Python, API development, web scraping, AI API integration, PostgreSQL, Docker  

- **STACK**:  
  Python, FastAPI, SQLAlchemy (async), PostgreSQL, BeautifulSoup, DeepSeek API, Docker  

---

## 8. 🍽️ [Restaurant Dish Detection – YOLOv11 Object Detector](https://github.com/Almas1989/comp_vision_proj)

- **Description**:  
  End-to-end object detection pipeline for recognizing 6 classes of dishes and tableware in restaurant settings. Built using YOLOv11 with custom training, evaluation, and video visualization. Data preparation includes manual annotation, augmentation, and class balancing.

- **Features**:  
  - Frame extraction from video (1 frame / 5 seconds)  
  - Manual annotation with LabelImg (YOLO format)  
  - 6 dish categories: tea, salads, kebab, chicken steak, soup, empty dishes  
  - Data augmentation with Albumentations  
  - Two-stage training with YOLOv11n and evaluation via mAP, Precision, Recall, F1  
  - Output visualization in labeled video  
  - Result analysis with per-class metrics and error matrix  

- **Skills Practiced**:  
  Object detection, dataset creation, model training & tuning, metrics analysis, OpenCV, Albumentations, YOLOv11

- **STACK**:  
  Python, OpenCV, LabelImg, Albumentations, YOLOv11, PyTorch

---

⭐ If you find these projects helpful, please give the repository a star and feel free to connect!
