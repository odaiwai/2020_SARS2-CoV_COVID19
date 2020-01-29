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

my $verbose = 1;
my @getters = `ls get_*.p?`;
my @parsers = `ls process_*.p?`;	

while (my $arg = shift(@ARGV)) {
	if ( $arg =~ /verbose/ ) {
		$verbose = 1;
	}
}

print run_all_scripts(@getters);
print run_all_scripts(@parsers);

## end
sub run_all_scripts {
	my @scripts = @_;
	my $results = 0;
	for my $scrip (@scripts) {
		chomp $script;
		my $result = `./$script`;
		print "$result\n" if $verbose;
		$results ++;
	}

	return $results;
}

