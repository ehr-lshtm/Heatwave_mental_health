/*=========================================================================
DO FILE NAME:			3_depression_heat-japan-01-set-up-SCCS

AUTHOR:					Angel Wong	
						
VERSION:				v1
					
DATABASE:				Japanese claims data
	
DESCRIPTION OF FILE:	Identify study population using heat as an exposure in people with mental disorders and/or prescribed antipsychotics N05 or antidepressants N06

MORE INFORMATION:	

Identify first case within the observation period

Observation period defined as
start from the first day of diagnosis of mental disorder, study start date (1/1/2014) and end at death date or study end date 31/12/2021)
Exclude people with age <18 at the study start date
*=========================================================================*/

cap log close

* create a filename global that can be used throughout the file
global filename "3_depression_heat-japan-01-set-up-SCCS"

* open log file - no need as fast tool will create log files
log using "${pathLogs}/${filename}", text replace

/*************************************
*People with depression
**************************************/
use "$datafile/depression_dxall", clear
rename 加入者id patid
sort patid eventdate
bysort patid: keep if _n==1
unique patid
save "$datafile/depression_all", replace

* key dob, sex and city to the dataset
rename patid 加入者id
merge 1:1 加入者id using "Y:\Ibaraki_claims_data_10years\データセットforStata\母数.dta", ///
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

*set up observation period
gen st_st = max(eventdate, ob_st_date, d(01jan2014))
format st_st %td

*keep people with age >=18 at the start of follow-up
gen age_index = (st_st-dob)/365.25
drop if age_index <18
unique 加入者id

gen st_en = min(ob_end_date, d(31dec2021))
format st_en %td

*remove people with invalid observation period
drop if st_en < st_st
unique 加入者id

*key city category code to city code
merge m:1 city_code using "$datafile/city_codebook_v2", keep(master match) keepusing(city_cat_code) nogen

tab city_cat_code,m

forval city_num = 1/6 {
        preserve
        keep if city_cat_code == `city_num'
        save "depression_`city_num'", replace
        restore
}


*key the heatwave data to antipsychotics data stratified by 6 cities
forval city_num = 1/6 {

use "depression_`city_num'", clear
*set up risk period (heatwave)
gen obs=1
joinby obs using "$datafile/MEANT_heatwave_city`city_num'_date"
gen exposure = 1 if st_st<=hw_start & hw_start<=st_en
replace exposure = 1 if st_st<=hw_end & hw_end<=st_en

*Remove those who did not experience heatwave during observation period
bysort 加入者id: egen max_exposure = max(exposure)
keep if max_exposure == 1

gen exp_st = max(hw_start, st_st)
gen exp_en = min(hw_end, st_en)
format exp_st exp_en %td

drop if exp_st > exp_en

drop obs exposure max_exposure
 
rename 加入者id patid 

save "$datafile/depression_heatwave_`city_num'", replace
}

*combine all dataset into one
use "$datafile/depression_heatwave_1", clear
forval city_num = 2/6 {
append using "$datafile/depression_heatwave_`city_num'"
}
save "$datafile/depression_heatwave", replace


*erase unneccessary datasets 
forval city_num = 1/6 {
erase "$datafile/depression_heatwave_`city_num'.dta"
}

/*************************************
*People taking antipsychotics
**************************************/
use "$datafile/psychotropic_drug_N05andN06", clear
keep if substr(who_atc, 1, 4) == "N06A"

tab who_atc,m sort 
egen drug_type = group(who_atc)

preserve 
duplicates drop who_atc, force
list who_atc drug_type 
restore

save "$datafile/psychotropic_drug_N06A", replace

*save a record only have one patient per row
duplicates drop 加入者id, force
save "$datafile/psychotropic_drug_N06A_hc", replace

*generate a dataset to help identify if they have multiple antipsychotic drugs
use "$datafile/psychotropic_drug_N06A", clear
unique 加入者id
drop if year(rxst) < 2014 | year(rxst) > 2021

preserve
sort 加入者id rxst rxen drug_type
bysort 加入者id rxst rxen drug_type: keep if _n==1
save "psychotropic_drug_N06A_rd", replace
restore

*identify all antipsychotics among psychiatric patients
*handle overlapping prescriptions
rename 加入者id patid

*Handle prescription gaps
sort patid rxst rxen
keep patid rxst rxen
drop if rxst > rxen

by patid: gen episode=_n

rename rxst timerxst
rename rxen timerxen

*Start the steps of handling overlapping by reshaping the data
reshape long time, i(patid episode) j(start_end) string

*Encode the start and end for ranking the order for "rxst" first
gen start_end2= 0 if start_end=="rxst"
replace start_end2=1 if start_end=="rxen"

by patid (time start_end2), sort: gen int in_proc = sum(start_end == "rxst") - sum(start_end == "rxen")
replace in_proc = 1 if in_proc > 1
by patid (time): gen block_num = 1 if in_proc == 1 & in_proc[_n-1] != 1
by patid (time): replace block_num = sum(block_num)

by patid block_num (time), sort: assert start_end == "rxst" if _n == 1
by patid block_num (time): assert start_end == "rxen" if _n == _N
by patid block_num (time): keep if _n == 1 | _n == _N

drop episode in_proc start_end2
reshape wide time, i(patid block_num) j(start_end) string
rename time* *
order rxst, before(rxen)

by patid: gen episode=_n
keep patid episode rxst rxen

rename rxst timerxst
rename rxen timerxen

reshape long time, i(patid episode) j(start_end) string

gen start_end2= 0 if start_end=="rxst"
replace start_end2=1 if start_end=="rxen"

by patid (time start_end2), sort: gen gap_num = 1 if start_end == "rxst" & (time- time[_n-1]<=1) //change the number of days here
replace gap_num = 1 if start_end == "rxen" & gap_num[_n+1] == 1
egen gap_num_max=max(gap_num), by (patid episode)

keep if (gap_num_max==1 & gap_num==.) | (gap_num==. & gap_num_max ==.)

drop gap_num gap_num_max episode start_end2

*change the episode no as rx for reshaping the wide form
egen rx =seq(), f(1) b(2)
reshape wide time, i(patid rx) j(start_end) string
rename time* *
order rxst, before(rxen)

count 

keep patid rxst rxen

*Further combining Rxs for their gap >90 between Rxs 
by patid: gen episode=_n
keep patid episode rxst rxen

rename rxst timerxst
rename rxen timerxen

reshape long time, i(patid episode) j(start_end) string

gen start_end2= 0 if start_end=="rxst"
replace start_end2=1 if start_end=="rxen"

by patid (time start_end2), sort: gen gap_num = 1 if start_end == "rxst" & (time- time[_n-1]<=90) 
replace gap_num = 1 if start_end == "rxen" & gap_num[_n+1] == 1
egen gap_num_max=max(gap_num), by (patid episode)

keep if (gap_num_max==1 & gap_num==.) | (gap_num==. & gap_num_max ==.)

drop gap_num gap_num_max episode start_end2

*change the episode no as rx for reshaping the wide form
egen rx =seq(), f(1) b(2)
reshape wide time, i(patid rx) j(start_end) string
rename time* *
order rxst, before(rxen)

count 

keep patid rxst rxen

sort patid rxst rxen

rename patid 加入者id
rename rxst rxst_antipsy
rename rxen rxen_antipsy

merge m:1 加入者id using "$datafile/psychotropic_drug_N06A_hc", ///
keepusing (dob gender ob_st_date ob_end_date city_code death_flag) keep(match) nogen

rename 加入者id patid

*merge those with the psychiatric diagnostic date
merge m:1 patid using "$datafile/depression_all", keepusing(eventdate) keep(match) nogen

*set up observation period
gen st_st = max(eventdate, ob_st_date, d(01jan2014))
gen st_en = min(ob_end_date, d(31dec2021))
format st_st st_en %td

*keep people aged >=18 at the start of follow-up
gen age_index = (st_st-dob)/365.25
drop if age_index <18
unique patid

*remove people with invalid observation period & drug date outside observation period
drop if st_en < st_st
drop if rxen < st_st
drop if rxst > st_en

replace rxst = st_st if rxst < st_st
replace rxen = st_en if rxen > st_en
drop if rxst > rxen
unique patid

save "$datafile/depression_clean_drug_all", replace

/**********************************************************************
identify dataset containing people without antipsychotics using 
dataset containing people with antipsychotics
**********************************************************************/
use "$datafile/depression_clean_drug_all", clear
duplicates drop patid, force
tempfile psychiatric_drug
save `psychiatric_drug'

*people without antipsychotics
use "$datafile/depression_heatwave", clear
merge m:1 patid using `psychiatric_drug', keep(master) nogen
save "$datafile/depression_heatwave_noantipsy", replace

unique patid

log close
