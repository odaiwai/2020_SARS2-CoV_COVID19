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
my @getters = qw/get_3gdxy_data.py get_3gdxy_json.py get_jhu_data.sh get_press_releases.pl/;
my @parsers = qw/process_press_releases.pl process_ncor_2019_data.py/;

while (my $arg = shift(@ARGV)) {
	if ( $arg =~ /verbose/ ) { $verbose = 1; }
	if ( $arg =~ /silent/ )  { $verbose = 0; }
}
if ( $verbose ) {
	$options = "verbose";
}


print run_all_scripts(@getters);
#print run_all_scripts(@parsers);

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

