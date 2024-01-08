/*=========================================================================
DO FILE NAME:			11_psychotropic_heat-call-model-01-CCO-set-up

AUTHOR:					Angel Wong
VERSION:				v1
DATABASE:				Japanese claims data

Aim: To create datasets for running case-crossover studies
identify cases for each outcome 
*=============================================================================*/
/*identify cases*/

*generate a dataset containing all heatwave in all 6 regions with city_cat_code variable
use "$datafile/MEANT_heatwave_city1_date", clear
gen city_cat_code = 1
forval city_num=2/6 {
	append using "$datafile/MEANT_heatwave_city`city_num'_date"
replace city_cat_code = `city_num' if city_cat_code==. 
}
save "$datafile/MEANT_heatwave_allcity_date", replace

* Set up the dataset for running CCO
foreach drug in antipsy antidepress {
	foreach outcome in heat_illness myocardial_infarct delirium {
use "$datafile/antipsy_`outcome'_first", clear //both antipsy and antidepress use the same file

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

*generate observation end date
gen ob_end_date = mdy(ob_end_month,ob_end_day,ob_end_year)
format ob_end_date %td

drop observation_start ob_start_year ob_start_month ob_start_day observation_end ob_end_year ob_end_month ob_end_day

*key psychotropics data to make sure the patient were prescribed psychotropics before //from 8 do-file
merge 1:1 加入者id using "$datafile/`drug'_first_clean_drug_all", ///
keepusing(st_st rxst_antipsy rxen_antipsy) keep(match) nogen

rename 加入者id patid

*merge those with the psychiatric diagnostic date
merge m:1 patid using "$datafile/psychiatric_all", keepusing(eventdate) keep(match) nogen

*set up study start and end date for each patient
gen startdate = d(1jan2014)
gen enddate= d(31dec2021)
gen cohortentry=max(st_st, startdate, ob_st_date, rxst_antipsy)
gen cohortend=min(enddate, ob_end_date, rxen_antipsy)

format startdate enddate cohortentry cohortend %td

*limit to those who aged >=18 at the cohort entry
*Generate variable of age at index date
gen age_index=(cohortentry-dob)/365.25
keep if age_index>=18

keep if eventdate!=. & eventdate >= cohortentry & eventdate <= cohortend
sort patid eventdate
duplicates drop patid, force

merge m:1 city_code using "$datafile/city_codebook_v2", keep(master match) keepusing(city_cat_code) nogen

tab city_cat_code,m

save "$datafile/cco_`outcome'_format_`drug'", replace
	}
}


/*********************************************************************
* Run the case-crossover study do-file************* washout period using a week
*********************************************************************/
foreach drug in antipsy antidepress {
	foreach outcome in heat_illness myocardial_infarct delirium {
			foreach riskperiod in 5 {
				foreach washoutperiod in 11 {

global washoutperiod `washoutperiod'
global riskperiod `riskperiod'
global drug `drug'
global inputdataset "$datafile/cco_`outcome'_format_`drug'"
global exposure_dataset "$datafile/MEANT_heatwave_allcity_date"
global logname $pathLogs/CCO/2_cco_`drug'_`outcome'
global outputtable $writeup/CCO/table2_cco_`drug'_`outcome'.txt

cap noi do "$pathDofiles/10_heat-01-CCO-program.do"

			}
			}
		}
}
