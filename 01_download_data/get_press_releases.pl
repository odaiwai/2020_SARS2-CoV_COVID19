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
my $verbose = 0;
my $datadir = "01_download_data";
my $baseurl = "https://www.info.gov.hk/gia/general/";
my $keywords = "Wuhan|SARS|MERS|pneumonia|hospital|statistics";
my @yearmonth_days;
while (my $arg = shift) {
	if ( $arg =~ /([0-9]{6})([0-9]{2})/ ) {
		push @yearmonth_days, "$1/$2";
	}
}
# Always do today
my $yearmonth = `date +%Y%m`;
chomp $yearmonth;
my $day = `date +%d`;
chomp $day;
push @yearmonth_days, "$yearmonth/$day";

for my $yearmonth_day (@yearmonth_days) {
	my $pressrelease_url = "$baseurl/$yearmonth_day.htm";
	open (my $fh, "-|", "lynx -dump $pressrelease_url"); 
	my %links;
	while (my $line = <$fh>) {
		chomp $line;
		print "$line\n" if $verbose;
		if ( $line =~ /\[([0-9]+)\](.*($keywords).*)$/ ) {
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
				my $result = `cd $datadir; wget -nc $link_url`; 
			}
		}
	}
	close $fh;
}
