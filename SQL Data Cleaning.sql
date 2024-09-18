-- Data Cleaning Project for Layoffs Data

-- Step 1: Create a staging table for cleaning the data (preserving the raw data)
CREATE TABLE layoffs_staging 
LIKE layoffs;

-- Verify the contents of the staging table
SELECT * FROM layoffs_staging;

-- Insert raw data into the staging table
INSERT INTO layoffs_staging 
SELECT * FROM layoffs;

-- Step 1: Remove Duplicates

-- Identify potential duplicates based on specific columns
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Check for rows where duplicates exist (rows with row_num > 1)
WITH duplicates AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicates
WHERE row_num > 1;

-- Remove duplicates from the staging table using a CTE
WITH delete_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
DELETE
FROM delete_cte
WHERE row_num > 1;

-- Step 2: Create a new staging table for the cleaned data
CREATE TABLE layoffs_staging2 (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT DEFAULT NULL,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert cleaned data into the new staging table
INSERT INTO layoffs_staging2
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
FROM layoffs_staging;

-- Step 3: Standardize Data

-- Trim whitespace from company names
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Standardize industry values (example: 'Crypto' category)
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardize country values
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Convert date strings to DATE format
UPDATE layoffs_staging2 
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modify the `date` column to be of DATE type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step 4: Handle NULL or Blank Values

-- Delete rows where both `total_laid_off` and `percentage_laid_off` are NULL
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Replace blank `industry` values with NULL and fill missing `industry` data based on company matches
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Final clean table without the row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Verify the final cleaned data
SELECT * FROM layoffs_staging2;
