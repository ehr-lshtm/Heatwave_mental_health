/*=========================================================================
DO FILE NAME:			2_heat-japan-01-import-extract-data

AUTHOR:					Angel Wong
VERSION:				v1			
DATABASE:				Japanese claims data

Aim: To identify study population and outcome of interest for SCCS
*=============================================================================*/
cap log close

* create a filename global that can be used throughout the file
global filename "2_heat-japan-01-import-extract-data"

* open log file - no need as fast tool will create log files
log using "${pathLogs}/${filename}", text replace
/******************************************************************
Descriptive analysis on antipsychotic users
*****************************************************************/
*import city codebook to stata
import delimited "Z:\Angel\documentation\city_codebook_v2.txt", clear
drop if _n==1
rename 国民健康保険 city_code
rename 保険者名 city_name
rename v3 city_cat_code

label var city_cat_code "category of city"
label def city_cat 1 "Kitaibaraki" 2 "Hitachi" 3 "Mito" 4 "Kashima" 5 "Tsukuba" 6 "Shimotsuma"
label val city_cat_code city_cat
save "$datafile/city_codebook_v2", replace

/******************************************************************
Extract antipsyhotics drug exposure by Yuta using SQL
information from Z:\Angel\antipsy\datafile\FromYuta
******************************************************************/
use "$datafile/FromYuta/231129_fromYuta_who_atc\231129_drug_N05andN06_v2.dta", clear
 
rename 加入者id patid
rename レセ種別コード type_of_claim  
rename atc中分類コード atc_3digit         
rename atc小分類コード atc_4digit               
rename atc細分類コード atc_detail
rename whoatcコード who_atc         
rename 医薬品名 drug_name              
rename ブランド名 brand_name                
rename 規格単位 standardised_unit         
rename 処方日 prescription_start_date
rename 調剤日 dispensing_date                   
rename 処方あたりの1日投与量 daily_dose                  
rename 処方あたりの投与日数 duration         

keep patid type_of_claim atc_3digit atc_4digit atc_detail who_atc drug_name brand_name standardised_unit prescription_start_date dispensing_date daily_dose duration

*format the variable
tostring(prescription_start_date), gen(prescription_date_str)
gen prescription_date = date(prescription_date_str, "YMD")
tostring(dispensing_date), gen(dispensing_date_str)
gen rxst = date(dispensing_date_str, "YMD")
gen rxen = rxst + duration - 1
format rxst rxen prescription_date %td    

drop prescription_start_date dispensing_date prescription_date_str dispensing_date_str

* check drug name 
tab who_atc, m sort

* key dob, sex and city to the dataset
rename patid 加入者id
merge m:1 加入者id using "Y:\Ibaraki_claims_data_10years\データセットforStata\母数.dta", ///
keepusing(加入者生年月日付補完 加入者性別 観察開始年月 観察終了年月 保険者番号 観察終了理由死亡フラグ) keep(master match) nogen

rename 加入者生年月日付補完 dob
rename 加入者性別 gender
rename 観察開始年月 observation_start
rename 観察終了年月 observation_end
rename 保険者番号 city_code
rename 観察終了理由死亡フラグ death_flag

tostring dob, replace
tostring observation_start, replace
tostring observation_end, replace

*format the date of birth
gen dob_format = date(dob, "YMD")
format dob_format %td
drop dob
rename dob_format dob

*format the observation start and end date
*set the start date as the end date of the month (2012, 2016 and 2020 are the leap year)
gen ob_start_year = substr(observation_start, 1, 4)
gen ob_start_month = substr(observation_start, 5, 6)

gen ob_start_day = 31 if ob_start_month == "01" | ///
 ob_start_month == "03" | ob_start_month == "05" | ob_start_month == "07" | ///
  ob_start_month == "08" | ob_start_month == "10" | ob_start_month == "12" 
  
replace ob_start_day = 30 if ob_start_month == "04" | ///
 ob_start_month == "06" | ob_start_month == "09" | ob_start_month == "11" 
 
replace ob_start_day = 28 if ob_start_month == "02"
 
replace ob_start_day = 29 if ob_start_month == "02" & ///
(ob_start_year == "2012" | ob_start_year == "2016" | ob_start_year == "2020")

destring ob_start_year, replace
destring ob_start_month, replace

*generate observation start date
gen ob_st_date = mdy(ob_start_month,ob_start_day,ob_start_year)
format ob_st_date %td

*set the end date as the start date of the month (2012, 2016 and 2020 are the leap year)
gen ob_end_year = substr(observation_end, 1, 4)
gen ob_end_month = substr(observation_end, 5, 6)

gen ob_end_day = 1 

destring ob_end_year, replace
destring ob_end_month, replace

*generate observation start date
gen ob_end_date = mdy(ob_end_month,ob_end_day,ob_end_year)
format ob_end_date %td

drop observation_start ob_start_year ob_start_month ob_start_day observation_end ob_end_year ob_end_month ob_end_day

save "$datafile/psychotropic_drug_N05andN06", replace

/******************************************************************
Extract outcomes from dignosis datasets
*****************************************************************/
*Heat related illness

*extract records from all diagnosis dataset
forval year = 2012/2021 {
use 加入者id icd10細分類コード 標準病名 診療開始年月日 using "$pathIn/医科_傷病_`year'.dta", clear
rename icd10細分類コード icd10
keep if substr(icd10,1,3)=="T67" | substr(icd10,1,3)=="E86" | ///
 substr(icd10,1,3)=="E87" | substr(icd10,1,3)=="X30"
 
 tostring 診療開始年月日, gen(disease_date)
gen eventdate = date(disease_date, "YMD")
format eventdate %td

rename 標準病名 term
drop 診療開始年月日 disease_date
save "$datafile/antipsy_heat_illness_`year'", replace
}

*combine datasets
use "$datafile/antipsy_heat_illness_2012", clear
forval year = 2013/2021 {
	append using "$datafile/antipsy_heat_illness_`year'"
}
save "$datafile/antipsy_heat_illness_all", replace

*only keep the first record ever
use "$datafile/antipsy_heat_illness_all", clear
sort 加入者id eventdate
bysort 加入者id: keep if _n==1
rename 加入者id patid
save "$datafile/antipsy_heat_illness_first", replace

*keep multiple record but 30 days apart between 2 records
use "$datafile/antipsy_heat_illness_all", clear
duplicates drop 加入者id eventdate, force
*generate day difference between 2 records
sort 加入者id eventdate
bysort 加入者id: gen day_diff = eventdate[_n] - eventdate[_n-1] 
bysort 加入者id: gen cum_daydiff = sum(day_diff)
drop if cum_daydiff < 30 & cum_daydiff > 0
rename 加入者id patid
save "$datafile/antipsy_heat_illness_multiple", replace

*myocardial infarction

*extract records from all diagnosis dataset
forval year = 2012/2021 {
use 加入者id icd10細分類コード 標準病名 診療開始年月日 using "$pathIn/医科_傷病_`year'.dta", clear
rename icd10細分類コード icd10
keep if substr(icd10,1,3)=="I21" | substr(icd10,1,3)=="I22" | ///
substr(icd10,1,3)=="I23" 

tostring 診療開始年月日, gen(disease_date)
gen eventdate = date(disease_date, "YMD")
format eventdate %td

rename 標準病名 term
drop 診療開始年月日 disease_date

save "$datafile/antipsy_myocardial_infarct_`year'", replace
}

*combine datasets
use "$datafile/antipsy_myocardial_infarct_2012", clear
forval year = 2013/2021 {
	append using "$datafile/antipsy_myocardial_infarct_`year'"
}
save "$datafile/antipsy_myocardial_infarct_all", replace

*only keep the first record ever
use "$datafile/antipsy_myocardial_infarct_all", clear
sort 加入者id eventdate
bysort 加入者id: keep if _n==1
rename 加入者id patid
save "$datafile/antipsy_myocardial_infarct_first", replace

*keep multiple record but 30 days apart between 2 records
use "$datafile/antipsy_myocardial_infarct_all", clear
duplicates drop 加入者id eventdate, force
*generate day difference between 2 records
sort 加入者id eventdate
bysort 加入者id: gen day_diff = eventdate[_n] - eventdate[_n-1] 
bysort 加入者id: gen cum_daydiff = sum(day_diff)
drop if cum_daydiff < 30 & cum_daydiff > 0
rename 加入者id patid
save "$datafile/antipsy_myocardial_infarct_multiple", replace

*delirium

*extract records from all diagnosis dataset
forval year = 2012/2021 {
use 加入者id icd10細分類コード 標準病名 診療開始年月日 using "$pathIn/医科_傷病_`year'.dta", clear
rename icd10細分類コード icd10
keep if substr(icd10,1,3)=="F05" | ///
substr(icd10,1,4)=="R410" | ///
substr(icd10,1,4)=="R400"

tostring 診療開始年月日, gen(disease_date)
gen eventdate = date(disease_date, "YMD")
format eventdate %td

rename 標準病名 term
drop 診療開始年月日 disease_date

save "$datafile/antipsy_delirium_`year'", replace
}

use "$datafile/antipsy_delirium_2012", clear
forval year = 2013/2021 {
	append using "$datafile/antipsy_delirium_`year'"
}
save "$datafile/antipsy_delirium_all", replace

*only keep the first record ever
use "$datafile/antipsy_delirium_all", clear
sort 加入者id eventdate
bysort 加入者id: keep if _n==1
rename 加入者id patid
save "$datafile/antipsy_delirium_first", replace

*keep multiple record but 30 days apart between 2 records
use "$datafile/antipsy_delirium_all", clear
duplicates drop 加入者id eventdate, force
*generate day difference between 2 records
sort 加入者id eventdate
bysort 加入者id: gen day_diff = eventdate[_n] - eventdate[_n-1] 
bysort 加入者id: gen cum_daydiff = sum(day_diff)
drop if cum_daydiff < 30 & cum_daydiff > 0
rename 加入者id patid
save "$datafile/antipsy_delirium_multiple", replace

*remove unncessary datasets
foreach outcome in heat_illness myocardial_infarct nms stroke {
	forval year = 2012/2021 {
		erase "$datafile/antipsy_`outcome'_`year'.dta"
	}
}
*/
/******************************************************************
Extract study population from dignosis datasets
******************************************************************/
*dignosis
*import codelist to STATA
foreach disease in  depression {
import excel using "Z:\Angel\documentation\ICD10_ATC_CODES_ZW.xlsx", sheet("icd10_`disease'") firstrow allstring clear
gen icd_3 = substr(icd,1,3)
gen icd_5 = substr(icd,5,5)
gen icd10 = icd_3 + icd_5 if !missing(icd_5)
replace icd10 = icd_3 if missing(icd_5)
drop icd_3 icd_5
save "$pathCodelists/icd10_`disease'", replace
					}

*create icd10 codelists for 3 digits only					
foreach disease in  depression {
import excel using "Z:\Angel\documentation\ICD10_ATC_CODES_ZW.xlsx", sheet("icd10_`disease'") firstrow allstring clear
gen icd_3 = substr(icd,1,3)
gen icd_5 = substr(icd,5,5)
keep if missing(icd_5)
drop icd_3 icd_5
rename icd icd10
save "$pathCodelists/icd10_3digit_`disease'", replace
					}

*extract records from all diagnosis dataset for identifying co-morbidities
foreach disease in 	depression {
forval year = 2012/2021 {
use 加入者id icd10細分類コード 標準病名 診療開始年月日 using "$pathIn/医科_傷病_`year'.dta", clear
rename icd10細分類コード icd10
merge m:1 icd10 using "$pathCodelists/icd10_`disease'", keep(match) keepusing(icd10) nogen

tostring 診療開始年月日, gen(disease_date)
gen eventdate = date(disease_date, "YMD")
format eventdate %td

rename 標準病名 term
drop 診療開始年月日 disease_date

save "$datafile/`disease'_`year'", replace
}
					}

*combine datasets
foreach disease in 	depression {
use "$datafile/`disease'_2012", clear
forval year = 2013/2021 {
	append using "$datafile/`disease'_`year'"
}
save "$datafile/`disease'_all", replace
					}
					
			
*extract records from all diagnosis dataset using 3 digits codelists for identifying co-morbidities
foreach disease in depression {
forval year = 2012/2021 {
use 加入者id icd10小分類コード 標準病名 診療開始年月日 using "$pathIn/医科_傷病_`year'.dta", clear
rename icd10小分類コード icd10
merge m:1 icd10 using "$pathCodelists/icd10_3digit_`disease'", keep(match) keepusing(icd10) nogen

tostring 診療開始年月日, gen(disease_date)
gen eventdate = date(disease_date, "YMD")
format eventdate %td

rename 標準病名 term
drop 診療開始年月日 disease_date

save "threedigit_`disease'_`year'", replace
}
					}

*combine datasets
foreach disease in depression {
use "threedigit_`disease'_all", clear
forval year = 2013/2021 {
	append using "threedigit_`disease'_`year'"
}
save "threedigit_`disease'_all", replace
			}
			
			
*combine all diagnostic datasets using different strings
foreach disease in 	depression  {
use "threedigit_`disease'_all", clear
append using "$datafile/`disease'_all"
duplicates drop 加入者id icd10 term eventdate, force
save "$datafile/`disease'_dxall", replace
					}
					
*severe mental illness					
forval year = 2012/2021 {
use 加入者id icd10小分類コード 標準病名 診療開始年月日 using "$pathIn/医科_傷病_`year'.dta", clear
rename icd10小分類コード icd10
keep if substr(icd10,1,3) == "F20" | substr(icd10,1,3) == "F21" | ///
substr(icd10,1,3) == "F22" | substr(icd10,1,3) == "F25" | ///
substr(icd10,1,3) == "F30" | substr(icd10,1,3) == "F31"
save "smd_`year'", replace
}

*combine all diagnostic datasets using different strings
forval year = 2012/2021 {
use "smd_`year'", clear
tostring 診療開始年月日, gen(disease_date)
gen eventdate = date(disease_date, "YMD")
format eventdate %td

rename 標準病名 term
drop 診療開始年月日 disease_date
save "$datafile/smd_`year'", replace
					}
					
use "$datafile/smd_2012", clear
forval year = 2013/2021 {
	append using "$datafile/smd_`year'"
}
save "$datafile/smd_dxall.dta"
					
*erase unneccessary files
		forval year = 2012/2021 {
erase "$datafile/smd_`year'.dta"
erase "smd_`year'.dta"	
		}

log close