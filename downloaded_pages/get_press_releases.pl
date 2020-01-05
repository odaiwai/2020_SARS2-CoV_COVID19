#!/usr/bin/env perl 
#===============================================================================
#
#         FILE: get_press_releases.pl
#        USAGE: ./get_press_releases.pl  
#  DESCRIPTION: Get Press Releases from info.gov.hk 
#       AUTHOR: Dave OBrien (odaiwai), odaiwai@diaspoir.net
#      CREATED: 01/05/2020 07:03:13 PM
#===============================================================================
use strict;
use warnings;
use utf8;

# https://www.info.gov.hk/gia/general/202001/02.htm
my $baseurl = "https://www.info.gov.hk/gia/general/";

my $yearmonth = `date +%Y%m`;
chomp $yearmonth;
my $day = `date +%d`;
chomp $day;

my $pressrelease_url = "$baseurl/$yearmonth/$day.htm";
open (my $fh, "-|", "lynx -dump $pressrelease_url"); 
my %links;
while (my $line = <$fh>) {
	chomp $line;
	print "$line\n";
	if ( $line =~ /\[([0-9]+)\](.*(Wuhan|SARS|MERS).*)$/ ) {
		my $link_num = $1;
		my $link_title = $2;
		$links{$link_num} = $link_title;
		print "found link: $link_num: $link_title\n";
	}
	if ( $line =~ /^\s+([0-9]+)\.\s+(http.*)$/ ) {
		my $link_num = $1;
		my $link_url = $2;
		if ( exists($links{$link_num}) ) {
			print "Downloading $link_num \"$links{$link_num}\" from $link_url\n";
			my $result = `wget -nc $link_url`; 
		}
	}
}
close $fh;
