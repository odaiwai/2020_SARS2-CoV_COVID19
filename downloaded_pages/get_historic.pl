#!/usr/bin/env perl 
#===============================================================================
#
#         FILE: get_historic.pl
#        USAGE: ./get_historic.pl  
#  DESCRIPTION: Script to download the historic NID data
#       AUTHOR: Dave OBrien (odaiwai), odaiwai@diaspoir.net
#      CREATED: 01/05/2020 06:36:40 PM
#===============================================================================
use strict;
use warnings;
use utf8;

my $infile = "urls.dat";
open (my $fh, "<", $infile) or die "Can't open $infile $!";
while (my $line = <$fh>) {
	chomp $line;
	my ($year, $url) = split ", ", $line;
	#my $result = `wget -nc $url`;
	my $csv = `lynx -dump $url | grep csv | cut -d' ' -f5`;
	chomp $csv;
	print "Downloading $csv...\n";
	my $result = `wget -nc $csv`;
}

