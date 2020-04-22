#!/usr/bin/env perl 
#===============================================================================
#
#         FILE: run_everything.pl
#        USAGE: ./run_everything.pl  
#  DESCRIPTION: Script to run all of the get_* script and all of the parse_*
#  scripts. 
#       AUTHOR: Dave OBrien (odaiwai), odaiwai@diaspoir.net
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

my @getters = qw/get_3gdxy_data.py get_3gdxy_json.py get_jhu_data.sh
				 get_press_releases.pl get_disease_outbreak_news.pl 
				 get_hgis_data.sh /;
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
	run_all_scripts(@getters);
	# Run the JHU file separately as it requires a parameter
	`./get_ncor_2019_data.py UPDATE >>$logfile.log 2>&1`;
	`./produce_ncor_plots.py >>$logfile.log 2>&1`;
}

#run_all_scripts(@parsers);

my @plots = qw/World China Italy USA Iran Italy France Germany Taiwan Spain 
				Singapore Macau Vietnam Ireland Poland
				Switzerland Netherlands/;
push @plots, "Hong Kong";
push @plots, "United Kingdom";
push @plots, "South Korea";
push @plots, "Confirmed_since_start";
push @plots, "Recovered_since_start";
push @plots, "Deaths_since_start";
push @plots, "Confirmed_new_since_start";
push @plots, "Recovered_new_since_start";
push @plots, "Deaths_new_since_start";

my @variants = qw/.png _since_start.png/;

for my $plot (@plots) {
	foreach my $variant (@variants) {
		my $filename = "plots/$plot$variant";
		if ( -f $filename ) {
			my $size = ( -s $filename );
			print "$plot$variant exists: $size bytes\n" if $verbose;
			my $result = `cp "$filename" /var/www/www.diaspoir.net/html/health/COVID19/`;
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
		my $result = `./$script $options >>$logfile 2>&1`;
		print "$result\n" if $verbose;
		$results ++;
	}

	return $results;
}

