#!/usr/bin/env perl
#===============================================================================
#
#         FILE: run_everything.pl
#        USAGE: ./run_everything.pl
#  DESCRIPTION: Script to run all of the get_* script and all of the parse_*
#  scripts.
#       AUTHOR: Dave OBrien (odaiwai), [REDACTED]
#      CREATED: 01/28/2020 01:27:52 PM
#===============================================================================
use strict;
use warnings;
use utf8;

my $verbose = 0;
my $getters = 1;
my $options = "silent";
my $logfile = "ncorplots.log";

`date > $logfile`;

#my @parsers = qw/process_ncor_2019_data.py/;

while (my $arg = shift(@ARGV)) {
	if ( $arg =~ /verbose/ ) { $verbose = 1; }
	if ( $arg =~ /silent/ )  { $verbose = 0; }
	if ( $arg =~ /get/ )  { $getters = 1 - $getters; }
}
if ( $verbose ) {
	$options = "verbose";
}

# run all the getters
if ( $getters) {
# my @getters = qw/get_3gdxy_data.py get_3gdxy_json.py get_jhu_data.sh
# 					get_press_releases.pl get_disease_outbreak_news.pl
# 					get_hgis_data.sh get_covid_buildings_list.py
# 					get_immd_data.py get_local_situation.py/;
#
    my @getters = qw/get_WHO_data.py get_disease_outbreak_news.pl
					get_hgis_data.sh get_immd_data.py get_local_situation.py/;

	run_all_scripts(@getters);
	# Process the plots
	my $result = `./process_ncor_2019_data.py $options >>$logfile 2>>$logfile`;
	my $finish_code = `echo \$# `;
	#print "$finish_code";
	if ( $finish_code != 0 ) {
		print "$result";
		exit;
	}

	# Produce the Plots
	$result = `./produce_ncor_plots.py $options >>$logfile 2>>$logfile`;
	$finish_code = `echo \$# `;
	#print "$finish_code";
	if ( $finish_code != 0 ) {
		print "$result";
		exit;
	} else {
		`git add -f plots/Confirmed_since_start.png && git commit -m "Updated main plot" plots/Confirmed_since_start.png`;
		`git add -f plots/Confirmed_new_since_start.png && git commit -m "Updated main new plot" plots/Confirmed_new_since_start.png`;
	}
}

#run_all_scripts(@parsers);

my @plots = qw/World China Italy USA Iran Italy France Germany Taiwan Spain
				Singapore Macau Vietnam Ireland Poland
				Switzerland Netherlands/;
push @plots, "Hong Kong";
push @plots, "United Kingdom";
push @plots, "South Korea";
push @plots, "Confirmed";
push @plots, "Recovered";
push @plots, "Deaths";

my @variants = qw/.png _since_start.png _new_since_start.png
				  _per_million_since_start.png _per_million_new_since_start.png/;

for my $plot (@plots) {
	foreach my $variant (@variants) {
		my $filename = "plots/$plot$variant";
		if ( -f $filename ) {
			my $size = ( -s $filename );
			print "$plot$variant exists: $size bytes. copying..." if $verbose;
			my $result = `cp "$filename" /var/www/www.diaspoir.net/html/health/COVID19/`;
			print "done.\n" if $verbose;
		}
	}
}

## end
sub run_all_scripts {
	my @scripts = @_;
	my $results = 0;
	for my $script (@scripts) {
		chomp $script;
		print "Running $script $options...\n" if $verbose;
		my $result = `./$script $options >>$logfile 2>>$logfile`;
		my $finish_code = `echo \$# `;
		#print "$finish_code";
		if ( $finish_code != 0 or $verbose ) {
			print "$result";
			exit;
		}
		$results ++;
	}

	return $results;
}
