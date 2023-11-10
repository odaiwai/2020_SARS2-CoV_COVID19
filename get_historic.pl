#!/usr/bin/env perl
#===============================================================================
#
#         FILE: get_historic.pl
#        USAGE: ./get_historic.pl
#  DESCRIPTION: Script to download the historic NID data
#       AUTHOR: Dave OBrien (odaiwai), [REDACTED]
#      CREATED: 01/05/2020 06:36:40 PM
#===============================================================================
use strict;
use warnings;
use utf8;

my $infile = "urls.dat";
my $datadir = "01_download_data";

open (my $fh, "<", $infile) or die "Can't open $infile $!";
while (my $line = <$fh>) {
	chomp $line;
	my ($year, $url) = split ", ", $line;
	#my $result = `wget -nc $url`;
	if ($year ne 'Main') {
		my $csv = `cd 01_download_data; lynx -dump $url | grep csv | cut -d' ' -f5`;
		chomp $csv;
		print "Downloading $csv...\n";
		my $result = `wget --directory-prefix $datadir -nc $csv`;
	}
}
