/*=========================================================================
DO FILE NAME:			10_mental_heat-call-model-01-CCO-set-up

AUTHOR:					Angel Wong
VERSION:				v1
DATABASE:				Japanese claims data

Aim: To create datasets for running case-crossover studies
identify cases for each outcome 
*=============================================================================*/
cap log close

* create a filename global that can be used throughout the file
global filename "10_mental_heat-call-model-01-CCO-set-up"

* open log file - no need as fast tool will create log files
log using "${pathLogs}/${filename}", text replace

/*identify cases*/

/*************************************
*People with mental disorder
**************************************/
use "$datafile/psychiatric_all", clear
rename eventdate disease_date
save "$datafile/psychiatric_people", replace

use "$datafile/depression_dxall", clear
rename eventdate disease_date
sort 加入者id disease_date
duplicates drop 加入者id, force
save "$datafile/depression_people", replace

/*************************************
* Set up the dataset for running CCO
**************************************/
*severe mental illness
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

rename 加入者id patid
*key disease data to start the follow-up from diagnosis
merge 1:1 patid using "$datafile/psychiatric_people", keepusing(disease_date) keep(match) nogen

rename patid 加入者id

*set up study start for each patient
gen startdate = d(1jan2014)
gen cohortentry=max(disease_date, startdate, ob_st_date)

*Remove anyone who had psychotropics 180 days before the first prescription
merge 1:m 加入者id using "$datafile/psychotropic_drug_N05A", keepusing(rxst) keep(master match) nogen

gen excl = 1 if rxst!=. & cohortentry-180 <= rxst & rxst <= cohortentry
bysort 加入者id: egen max_excl = max(excl)

drop if max_excl==1
unique 加入者id

*identify right censor after start of antipsychotics of interest
replace rxst = . if rxst < cohortentry
bysort 加入者id: egen right_censor = min(rxst) 
format right_censor %td
duplicates drop 加入者id, force
drop max_excl rxst excl

rename 加入者id patid

*set up end date for each patient
gen enddate= d(31dec2021)
gen cohortend=min(enddate, ob_end_date, right_censor-1)
format startdate enddate cohortentry cohortend %td

*limit to those who aged >=18 at the cohort entry
*Generate variable of age at index date
gen age_index=(cohortentry-dob)/365.25
keep if age_index>=18

*remove people with invalid observation period
drop if cohortend < cohortentry

keep if eventdate!=. & eventdate >= cohortentry & eventdate <= cohortend
sort patid eventdate
duplicates drop patid, force

merge m:1 city_code using "$datafile/city_codebook_v2", keep(master match) keepusing(city_cat_code) nogen

tab city_cat_code,m

unique patid
di "`outcome'"
su age_index, detail
tab gender,m

save "$datafile/cco_psychiatric_`outcome'_format", replace
	}
	
*depression
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

*key disease data to start the follow-up from diagnosis
merge 1:1 加入者id using "$datafile/depression_people", keepusing(disease_date) keep(match) nogen

*set up study start for each patient
gen startdate = d(1jan2014)
gen cohortentry=max(disease_date, startdate, ob_st_date)

*Remove anyone who had psychotropics 180 days before the first prescription
merge 1:m 加入者id using "$datafile/psychotropic_drug_N06A", keepusing(rxst) keep(master match) nogen

gen excl = 1 if rxst!=. & cohortentry-180 <= rxst & rxst <= cohortentry
bysort 加入者id: egen max_excl = max(excl)

drop if max_excl==1
unique 加入者id

*identify right censor after start of antidepressants of interest
replace rxst = . if rxst < cohortentry
bysort 加入者id: egen right_censor = min(rxst) 
format right_censor %td
duplicates drop 加入者id, force
drop max_excl rxst excl

rename 加入者id patid

*set up end date for each patient
gen enddate= d(31dec2021)
gen cohortend=min(enddate, ob_end_date, right_censor-1)
format startdate enddate cohortentry cohortend %td

*limit to those who aged >=18 at the cohort entry
*Generate variable of age at index date
gen age_index=(cohortentry-dob)/365.25
keep if age_index>=18

*remove people with invalid observation period
drop if cohortend < cohortentry

keep if eventdate!=. & eventdate >= cohortentry & eventdate <= cohortend
sort patid eventdate
duplicates drop patid, force

merge m:1 city_code using "$datafile/city_codebook_v2", keep(master match) keepusing(city_cat_code) nogen

tab city_cat_code,m

unique patid
di "`outcome'"
su age_index, detail
tab gender,m

save "$datafile/cco_depression_`outcome'_format", replace
	}

	log close
	
/*********************************************************************
* Run the Case-crossover study do-file************* washout period using a week
*********************************************************************/
foreach disease in psychiatric depression {
	foreach outcome in heat_illness myocardial_infarct delirium {
			foreach riskperiod in 5 {
				foreach washoutperiod in 11 {

global washoutperiod `washoutperiod'
global riskperiod `riskperiod'
global drug `disease'
global inputdataset "$datafile/cco_`disease'_`outcome'_format"
global exposure_dataset "$datafile/MEANT_heatwave_allcity_date"
global logname $pathLogs/CCO/1_cco_`disease'_`outcome'
global outputtable $writeup/CCO/table1_cco_`disease'_`outcome'.txt

cap noi do "$pathDofiles/10_heat-01-CCO-program.do"

			}
			}
		}
}

