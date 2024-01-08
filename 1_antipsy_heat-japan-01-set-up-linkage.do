/*=========================================================================
DO FILE NAME:			1_antipsy_heat-japan-01-set-up-linkage

AUTHOR:					Angel Wong	
						
VERSION:				v1
					
DATABASE:				Japanese claims data
	
DESCRIPTION OF FILE:	identify heatwave from temperature dataset

DATASETS USED:		"$datafile/Temperature_2012Apr-2022Mar.xlsx" from Chitose

DATASETS CREATED: 	"$datafile/MEANT_heatwave_date"									

MORE INFORMATION:	1. Load the temperature data to Stata
					2. Identify heatwave dates using the temperature dataset

*=========================================================================*/
* load temperature data from Observatory to Stata
foreach city in Kitaibaraki Hitachi Mito Hokota Kashima Tsukuba Shimotsuma Ryugasaki {
import excel using "$datafile/Temperature_2012Apr-2022Mar.xlsx", ///
allstring cellrange(A5) clear sheet(`city')
gen date = date(A, "YMD")
format date %td

rename B mean_temp
rename C max_temp
rename D min_temp
drop A
destring mean_temp, replace
destring max_temp, replace
destring min_temp, replace

gen city = "`city'"
save "$datafile/temp_data_`city'", replace
}
*key the location indicator to the temperature data
use "$datafile/temp_data_Kitaibaraki", clear
foreach city in Hitachi Mito Kashima Tsukuba Shimotsuma {
append using "$datafile/temp_data_`city'"
}
gen location_num = 1 if city == "Kitaibaraki"
replace location_num = 2 if city == "Hitachi"
replace location_num = 3 if city == "Mito"
replace location_num = 4 if city == "Kashima"
replace location_num = 5 if city == "Tsukuba"
replace location_num = 6 if city == "Shimotsuma"

keep date mean_temp location_num
reshape wide mean_temp, i(date) j(location_num)
gen year= year(date)

save "$datafile/temp_data_six_city", replace

*import temperature data (6 cities) to stata 
forval num= 1/6 {
	
	use "$datafile/temp_data_six_city", clear
	keep date mean_temp`num' year 
	*drop days that are outside study period
	drop if date < d(01jan2014)
	drop if date > d(31dec2021)

*Use heatwave definition of >= 2 consecutive days with daily mean temperature 
*exceeding 95% percentile of the year round from recent data
*https://journals.plos.org/plosmedicine/article?id=10.1371/journal.pmed.1002629
*find the temperature 95% percentile for each year
preserve
keep if year == 2014
su mean_temp`num', detail
return list
restore

gen over_95_temp = 1 if year == 2014 & mean_temp`num' >= r(p95)

forval yr = 2015/2021 {
preserve
keep if year == `yr'
bysort year: su mean_temp`num', detail
return list
restore
replace over_95_temp = 1 if year == `yr' & mean_temp`num' >= r(p95)

}

sort date

gen heatwave_95 = 1 if over_95_temp==1 & (over_95_temp[_n-1]==1 | over_95_temp[_n+1]==1)

save "$datafile/MEANT_all_final_`num'", replace

*generate a heatwave dataset
use "$datafile/MEANT_all_final_`num'", clear
keep if heatwave_95 == 1 
clonevar date_end=date
rename date date_start
gen Referencekey = 1
sort Referencekey date_start date_end
by Referencekey: gen episode=_n

keep Referencekey date_start date_end episode

rename date_start timerxst
rename date_end timerxen

/************************************************************************
**************************************************************************
Start the steps of handling overlapping by reshaping the data
************************************************************************
*************************************************************************/
reshape long time, i(Referencekey episode) j(start_end) string

*Encode the start and end for ranking the order for "rxst" first
gen start_end2= 0 if start_end=="rxst"
replace start_end2=1 if start_end=="rxen"

by Referencekey (time start_end2), sort: gen int in_proc = sum(start_end == "rxst") - sum(start_end == "rxen")
replace in_proc = 1 if in_proc > 1
by Referencekey (time): gen block_num = 1 if in_proc == 1 & in_proc[_n-1] != 1
by Referencekey (time): replace block_num = sum(block_num)

by Referencekey block_num (time), sort: assert start_end == "rxst" if _n == 1
by Referencekey block_num (time): assert start_end == "rxen" if _n == _N
by Referencekey block_num (time): keep if _n == 1 | _n == _N

drop episode in_proc start_end2
reshape wide time, i(Referencekey block_num) j(start_end) string
rename time* *
order rxst, before(rxen)

by Referencekey: gen episode=_n
keep Referencekey episode rxst rxen

rename rxst timerxst
rename rxen timerxen

reshape long time, i(Referencekey episode) j(start_end) string

gen start_end2= 0 if start_end=="rxst"
replace start_end2=1 if start_end=="rxen"

by Referencekey (time start_end2), sort: gen gap_num = 1 if start_end == "rxst" & (time- time[_n-1]<=1) //change the number of days here
replace gap_num = 1 if start_end == "rxen" & gap_num[_n+1] == 1
egen gap_num_max=max(gap_num), by (Referencekey episode)

keep if (gap_num_max==1 & gap_num==.) | (gap_num==. & gap_num_max ==.)

drop gap_num gap_num_max episode start_end2

*change the episode no as rx for reshaping the wide form
egen rx =seq(), f(1) b(2)
reshape wide time, i(Referencekey rx) j(start_end) string
rename time* *
order rxst, before(rxen)

count 

keep rxst rxen

rename rxst hw_start
rename rxen hw_end

gen obs=1

save "$datafile/MEANT_heatwave_city`num'_date", replace
	
}

