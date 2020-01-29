#!/usr/bin/env perl 
#===============================================================================
#
#         FILE: get_disease_outbreak_news.pl
#        USAGE: ./get_disease_outbreak_news.pl  
#  DESCRIPTION: Get the Diease Outbreak News from the WHO - more reliable than
#  China/HK govt?
#       AUTHOR: Dave OBrien (odaiwai), odaiwai@diaspoir.net
#      CREATED: 01/20/2020 12:45:25 PM
#===============================================================================
use strict;
use warnings;
use utf8;
use WWW::Mechanize;


#my $baseurl = "https://www.who.int/csr/don/en/";
my $verbose = 1;
my $baseurl = "https://www.who.int/csr/don/archive/disease/novel_coronavirus/en/";
my $baseurl2 = "https://3g.dxy.cn/newh5/view/pneumonia?scene=2";

# process the options
my @wget_options = qw/-nc/;
push @wget_options, "-v" if $verbose;
push @wget_options, "-q" if !($verbose);
my $wget_options = join( " ", @wget_options);

# Situation Reports from WHO
#my @sitreps = `lynx -dump https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports/ | grep pdf | cut -d' ' -f3`;
my @sitreps = `lynx -dump https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports/ | grep pdf | cut -d' ' -f3`;
for my $sitrep (@sitreps) {
	chomp $sitrep;
	$sitrep = sanitise_url($sitrep);
	my $cmd = "wget $wget_options $sitrep";
	print "$cmd\n";
	my $result = `cd 01_download_data; $cmd`;
	print "$result\n";
}




# SUBS
sub sanitise_url {
	my $url = shift;
	$url =~ s/\?/\\\?/g;
	return $url;
}

