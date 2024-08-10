
-- Data Cleaning Steps for "Layoffs" Dataset

-- Create a staging table for data manipulation

CREATE TABLE layoffs_staging LIKE layoffs;


INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


-- Check for duplicates in the staging table using ROW_NUMBER()

SELECT *, 
ROW_NUMBER() OVER(PARTITION BY COMPANY) AS row_num
FROM layoffs_staging;



-- Add new column row_num to identify duplicates and remove them

ALTER TABLE layoffs_staging ADD row_num INT;


-- create another staging table 

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` text,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;



-- Remove duplicate rows based on row_num

DELETE FROM layoffs_staging2
WHERE row_num >= 2;



-- as 'Crypto' value in industry column has multiple different variations. We need to standardize that
-- Standardize industry names to "Crypto"

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');


-- removing any space character from the start or end of a string using TRIM() function

UPDATE layoffs_staging2
SET company = TRIM(company);


-- Convert the data type of the date column to DATE 

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;


-- Identifying and handling missing valuse

SELECT *
 FROM layoffs_staging2
 WHERE total_laid_off = '';


UPDATE layoffs_staging2
 SET total_laid_off = 'Unknown'
 WHERE total_laid_off = '';


-- Remove rows with missing data in critical columns

DELETE FROM layoffs_staging2
WHERE total_laid_off = 'Unknown'
AND percentage_laid_off = '';

 
-- remove 'row_num' column as the duplicates are removed now

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
 