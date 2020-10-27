### How this file works
''' How to use this file
Each entry is a sorted list of versions of the dataset, from most accurate to least
'''

datasets = {
	"prism_precip" : [
		"prism_rev_6_precip-daily",
		"prism_rev_3_precip-daily",
		#"prism_rev_1_precip-daily"
	],
	"prism_temp" : [
		"prism_rev_6_temp-daily",
		"prism_rev_3_temp-daily",
		"prism_rev_1_temp-daily"
	],
	"chirps_05": [
		"chirps_05-daily",
		"chirps_prelim_05-daily"
	],
	"chirps_25": [
		"chirps_25-daily"
	],
	"cpc_global": [
		"cpc_global-daily"
	],
	"cpc_us": [
		"cpc_us-daily"
	],
	"cpc_temp": [
		"cpc_temp-daily"
	]
}
