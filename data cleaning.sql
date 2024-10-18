# data cleaning
-- 1.remove duplicates
-- 2.standardize the data
-- 3.null values or blank values
-- 4.remove any irrelevant columns 
  

select * from layoffs;

create table layoffs_staging
like layoffs;

insert into layoffs_staging
select * from layoffs;

-- removing duplicates

select * ,
row_number() over(
partition by company,location,industry,total_laid_off,`date`,stage,country,funds_raised_millions)
as row_num
from layoffs_staging;

 With duplicate_cte as
 (select * ,
row_number() over(
partition by company,location,industry,total_laid_off,`date`,stage,country,funds_raised_millions)
as row_num from layoffs_staging)
select * from duplicate_cte
where row_num >1;

select * from layoffs_staging where company= 'yahoo';

 With duplicate_cte as
 (select * ,
row_number() over(
partition by company,location,industry,total_laid_off,`date`,stage,country,funds_raised_millions)
as row_num from layoffs_staging)
delete from duplicate_cte
where row_num >1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert into layoffs_staging2
select * ,
row_number() over(
partition by company,location,industry,total_laid_off,`date`,stage,
country,funds_raised_millions)
as row_num from layoffs_staging;

delete from layoffs_staging2
where row_num >1;

select * from layoffs_staging2
where row_num >1;

-- standardize data

select company, trim(company)
 from layoffs_staging2;
 
 update layoffs_staging2
 set company=trim(company);
 
 select *
 from layoffs_staging2 ;
 
 select distinct(industry) 
 from layoffs_staging2 order by 1;
 
 update layoffs_staging2
 set industry = 'Crypto'
 where industry like 'Crypto%';
 
 select *
 from layoffs_staging2 where industry like 'Crypto%';
 
 -- check all columns for error by (select distinct columnname from layoffs_staging2 
-- order by 1)

select distinct country 
from layoffs_staging2
order by 1;
 
 select *
 from layoffs_staging2 where country like 'united states.%';
 
 update layoffs_staging2
 set country = trim(trailing '.' from country)
 where country like 'United States%';
 
 select `date`,
 str_to_date(`date`,'%m/%d/%Y')
 from layoffs_staging2;
 
 update layoffs_staging2
 set `date`=str_to_date(`date`,'%m/%d/%Y');
 
 alter table layoffs_staging2
 modify column `date` date;
 
 -- remove null or blank values
 
 select *
 from layoffs_staging2
 where total_laid_off is NULL 
 and percentage_laid_off is NULL;
 
 select *
 from layoffs_staging2
 where industry is null
 or industry = '';
 
 select *
 from layoffs_staging2 
 where company like 'Bally%';
 
 update layoffs_staging2
 set industry= NULL
 where industry='';
 
select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
	and t1.location=t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;
 
 
 update layoffs_staging2 t1
 join layoffs_staging2 t2
	on t1.company=t2.company
    and t1.location=t2.location
 set t1.industry=t2.industry
 where t1.industry is null 
 and t2.industry is not null;
 
 delete from layoffs_staging2
 where total_laid_off is NULL 
 and percentage_laid_off is NULL;
 
 alter table layoffs_staging2
 drop column row_num;