-- Data Cleaning

SELECT *
FROM layoffs; 

CREATE TABLE layoffs_staging
LIKE layoffs; 

SELECT *
FROM layoffs_staging; 

INSERT layoffs_staging
SELECT *
FROM layoffs; 

-- 1. Remove Duplicates 
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging; 

WITH duplicate_cte AS 
(SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- 2. Standardise the Data 
SELECT TRIM(company)
FROM layoffs_staging;

UPDATE layoffs_staging
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY 1;

SELECT *
FROM layoffs_staging
WHERE country LIKE 'United States%';

UPDATE layoffs_staging
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging
ORDER BY 1;

SELECT `date`, STR_TO_DATE(`date`, '%d/%m/%Y')
FROM layoffs_staging;
 
UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%d/%m/%Y');

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;

-- 3. Null values or blank values 
SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging
WHERE company = 'Bally\'s Interactive';

SELECT *
FROM layoffs_staging
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging t1
JOIN layoffs_staging t2
  ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging
SET industry = 'Travel'
WHERE company LIKE 'Airbnb';

-- 4. Remove unnecessary columns 
SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging;

-- Exploratory Data Analysis
SELECT MAX(total_laid_off)
FROM layoffs_staging;

SELECT *
FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging
GROUP BY YEAR(`date`)
ORDER BY 2 DESC; 

SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off)
FROM layoffs_staging
GROUP BY `MONTH`
ORDER BY 2 DESC;

WITH Rolling_Total AS 
(SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off) AS total_off
FROM layoffs_staging
GROUP BY `MONTH`
ORDER BY 2 DESC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

SELECT location, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY location
ORDER BY 2 DESC;

SELECT company, YEAR(`date`) AS years, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY company,years
ORDER BY 3 DESC;

WITH Company_Year (Company, Years, Total_Laid_off) AS
(SELECT company, YEAR(`date`) AS years, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY company,years),
Company_Year_Rank AS 
(SELECT *, DENSE_RANK() OVER(PARTITION BY Years ORDER BY Total_Laid_off DESC) AS Ranking
FROM Company_Year
ORDER BY Ranking ASC)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <=5;





