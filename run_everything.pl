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
my $options = "silent";
my @getters = qw/get_3gdxy_data.py get_3gdxy_json.py get_jhu_data.sh
				 get_press_releases.pl get_disease_outbreak_news.pl 
				 get_hgis_data.sh /;
#my @parsers = qw/process_ncor_2019_data.py/;

while (my $arg = shift(@ARGV)) {
	if ( $arg =~ /verbose/ ) { $verbose = 1; }
	if ( $arg =~ /silent/ )  { $verbose = 0; }
}
if ( $verbose ) {
	$options = "verbose";
}

run_all_scripts(@getters);
`./get_ncor_2019_data.py UPDATE 1>ncorplots.log `;

#run_all_scripts(@parsers);

my @plots = qw/World China Italy USA Iran Italy France Germany Taiwan Spain 
				All_CFR All_Confirmed Singapore Macau Vietnam Ireland Poland/;
push @plots, "Hong Kong";
push @plots, "United Kingdom";
push @plots, "South Korea";
push @plots, "Confirmed_since_start";
push @plots, "Recovered_since_start";
push @plots, "Dead_since_start";

for my $plot (@plots) {
	my $result = `cp "./plots/$plot.png" /var/www/www.diaspoir.net/html/health/COVID19/`;
}

## end
sub run_all_scripts {
	my @scripts = @_;
	my $results = 0;
	for my $script (@scripts) {
		chomp $script;
		print "Running $script $options...\n" if $verbose;
		my $result = `./$script $options`;
		print "$result\n" if $verbose;
		$results ++;
	}

	return $results;
}

