-- DATA CLEANING PROJECT #1 [SQL]


select*
from layoffs;

-- We will now create a table for staging jisme hum apna changes karenge, so that raw data doesn't get affected

create table layoff_staging
like layoffs;

-- now we will add the data

insert layoff_staging
select*
from layoffs;

select*
from layoff_staging;

-- LET'S SELECT DUPLICATES
# so what we will do is assign an unique row number to every row, jab there will be duplicates, the row number's value will be > 1

select*,
row_number () over (partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoff_staging;


-- we will copy the table to clipboard---> create statement, basically to create anoyther table with another column called row_number



CREATE TABLE `layoff_staging2` (
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


select*
from layoff_staging2;

-- exactly what we wantded

# now, let's put the data

insert into layoff_staging2
select *,
row_number () over (partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoff_staging;

-- after running this we caan see, it clearly works


select*
from layoff_staging2
where row_num > 1;

delete
from layoff_staging2
where row_num > 1;

select*
from layoff_staging2
;

-- so all the duplicates are removed, it would have been a lot easier if we had unique column row (like we used to have employee_id previously)




-- STANDARDIZING DATA

select distinct company, trim(company)
from layoff_staging2;

-- uniformity aa gaya, so we will keep this uniform coulmn now, so what we will do is update the table (go to edit---> preference and sql editor and check the last checkbox)

update layoff_staging2
set company = trim(company);

select distinct industry, trim(industry)
from layoff_staging2
order by 1
;



# thoda check karna phir, like i found crypto, crypto currency as 2 different values, so unko ek karna hai

update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

#(this converts all crypto..... whatever to crypto)

select distinct industry
from layoff_staging2;

update layoff_staging2
set country = 'United States'
where industry like 'United states%';


# This didn't work so

# we will do an advanced function here

select distinct country, trim(trailing '.' from country)
from layoff_staging2
order by 1; 

update layoff_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

# now let's fix the date


# note date here is a unique column so use `date` for it to work, son't use ''  !

# DATE FORMATTING

select `date`,
str_to_date (`date`, '%m/%d/%Y')
from layoff_staging2;

update layoff_staging2
set `date` = str_to_date (`date`, '%m/%d/%Y');

# we can see the date column is a text type, so we have to change it into a date type (never do it with the raw data though)

alter table layoff_staging2
modify column `date` date;

# you can check in the schemas now that the data type for date has been changed

select*,
row_number () over (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as new_row
from layoff_staging2
;



CREATE TABLE `layoff_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` date DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int DEFAULT NULL,
  `new_row` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select*
from layoff_staging3;

insert into layoff_staging3
select*,
row_number () over (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as new_row
from layoff_staging2
;


delete
from layoff_staging3
where new_row >1
;

# My bad, now the data has been cleaned properly, all the duplicates are removed.
# NOTE: as the data was taken from layoff_staging2, the previously done procedures are taken care of.

-- Let's fix the null values now

select*
from layoff_staging3
where industry is null
or industry = ''
;


select*
from layoff_staging3
where company = 'Airbnb'
;

# we can see, airbnb belongs to travel industry, so how to do it

select*
from layoff_staging3 as t1
join layoff_staging3 as t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null
;


select t1.industry, t2.industry
from layoff_staging3 as t1
join layoff_staging3 as t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null
;

# so what we will do is update the 1st column with the value given in column 2 (if there exists any)

update layoff_staging3
join layoff_staging3 as t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null
; 

#same query bas update set beech mai ghusaya hai 

# ek problem hai, in both tables bahut jagah empty hein, so jab ek row mai dono hi empty hein it might cause trouble. what we can do is convert empty values to null


update layoff_staging3
set industry = null
where industry = ''
;

# now it is fixed, now we copy the above syntax, (remove the 'industry = '' as they have been convverted to null')


select t1.industry, t2.industry
from layoff_staging3 as t1
join layoff_staging3 as t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null
;
update layoff_staging3 as t1
join layoff_staging3 as t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null
; 

select company, industry
from layoff_staging3
where company = 'Airbnb'
;

# As you can see, now it  is fixed

# bally's still not fixed

select *
from layoff_staging3
where company like 'Bally%'
;

# as only 1 row was there so there is no reference to copy from 

# ab jitna ho sakta tha wo to ho gaya, let's delete rows which have 2 or more null values

select *
from layoff_staging3
where total_laid_off is null
and percentage_laid_off is null
;

delete
from layoff_staging3
where total_laid_off is null
and percentage_laid_off is null
;

# All of them are cleared now

# let's delete the extra columns we created

ALTER TABLE layoff_staging3
drop column row_num ;


ALTER TABLE layoff_staging3
drop column new_row;

select*
from layoff_staging3;



--    											 	-----		THE  		END   			------


-- EDA PROJECT (Exploratory Data Analysis)

# We will explore the dataset and look for insights

select*
from layoff_staging3;

# Let's see the total laid off based on various parameters

# total laid_off in different countries
select  sum(total_laid_off), country
from layoff_staging3
group by country
order by 1 desc
;


# total laid_off company wise
select  company, sum(total_laid_off)
from layoff_staging3
group by company
order by 2 desc
;


# total laid_off industry wise
select  sum(total_laid_off), industry
from layoff_staging3
group by industry
order by 1 desc
;

# average laid_off industry wise

select  avg(total_laid_off), industry
from layoff_staging3
group by industry
order by 1 desc
;

# average percentage laid_off industry wise in the year 2023

select  avg(percentage_laid_off), industry
from layoff_staging3
where year(`date`) = 2023
group by industry
order by 1 desc
;



# average percentage laid_off industry wise in the year 2022

select  avg(percentage_laid_off), industry
from layoff_staging3
where year(`date`) = 2022
group by industry
order by 1 desc
;


# average percentage laid_off industry wise in the year 2021

select  avg(percentage_laid_off), industry
from layoff_staging3
where year(`date`) = 2021
group by industry
order by 1 desc
;


# average percentage laid_off industry wise in the year 2020

select  round(avg(percentage_laid_off),2) as average_percentage_laidoff, industry
from layoff_staging3
where year(`date`) = 2020
group by industry
order by 1 desc
;


# suppose we have to check month wise data, to use sub-string function

select substring(`date`,1,7) as `month`, round(avg(percentage_laid_off),2) as average_percentage_laidoff , sum(total_laid_off)
from layoff_staging3
where substring(`date`,1,7) is not null
group by substring(`date`,1,7)
order by 1 desc
;

#rolling total

with rolling_total as
(
select substring(`date`,1,7) as `month`, round(avg(percentage_laid_off),2) as average_percentage_laidoff , sum(total_laid_off) as total_sum
from layoff_staging3
where substring(`date`,1,7) is not null
group by substring(`date`,1,7)
)
select `month`, average_percentage_laidoff , sum(total_sum) over (order by `month`) as rolling_sum, total_sum
from rolling_total
;

 

# If we were to see the top 10 for each year, we will just make another cte in the existing cte 

with company_year as
(
select company, year(`date`) as year, round(avg(percentage_laid_off),2) as average_percentage_laidoff, sum(total_laid_off) as total_off
from layoff_staging3
group by company, year
),
# we will use another cte to get top 10 ranks
company_rank as
(
select *,
dense_rank () over (partition by year order by total_off desc) as Ranking
from company_year
where year is not null
and total_off is not null
)
select*
from company_rank
where Ranking <= 10
;
