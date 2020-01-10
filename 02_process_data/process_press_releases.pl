#!/usr/bin/env perl 
#===============================================================================
#
#         FILE: process_nid_data.pl
#        USAGE: ./process_nid_data.pl  
#  DESCRIPTION: Script to  parse the historic Press Releases and build a 
#  Database
#       AUTHOR: Dave OBrien (odaiwai), odaiwai@diaspoir.net
#      CREATED: 01/06/2020 01:52:27 PM
#===============================================================================
use strict;
use warnings;
use utf8;
use DBI;
use Data::Dumper;

## my DBHelper
use lib "/home/odaiwai/src/dob_DBHelper";
use DBHelper;

my $verbose = 1;
my $dbverb  = 1;
my $firstrun = 1;
my $datadir = "../01_download_data";
my @files = `ls $datadir/P*.htm`;
my $db = dbconnect("press_releases.sqlite", "", "") or die $DBI::errstr;
my @months = qw/Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec Total/;
my $columns = "Year, " . join(", ", @months);
my $coldefs = "Year INTEGER UNIQUE PRIMARY KEY, ".join( ", ", map { $_ ." Integer"} @months);
my %diseases;

print "@months\n";
print "$columns\n";
print "$coldefs\n";

if ( $firstrun) {
	#Make Tables
	my $result = drop_all_tables($db, "", $dbverb);
	my $fields = "Timestamp Integer Unique Primary Key, Date Integer, pr_num Integer, new_cases Integer, total Integer, discharged Integer";
	my @locations = qw/HongKong Wuhan/;

	my %tables = (
		"Location" => "name Text UNIQUE Primary Key",
	);
	foreach my $location (@locations) {
		$tables{$location} = $fields;
	}

	my $tables = make_db($db, \%tables, $dbverb);
	print "$tables tables initialised.\n";
}

#print "Processing @files...\n" if $verbose;

dbdo($db, "BEGIN", $dbverb);
for my $file (@files) {
	chomp $file;
	#print "Processing file: $file...\n";

	if ($file =~ /P([0-9]{8})([0-9]+)\.htm/) {
		my $date = $1;
		my $pr_num = $2;
		my $timestamp = "$1$2";
		my $table;
		my $fields = "Timestamp, Date, pr_num, new_cases, stable, discharged";
		my ($new_cases, $total, $discharged) = (0, 0, 0);

		open (my $fh, "-|", "lynx -dump $datadir/$file") or die "Can't open $datadir/$file. $!";
		my $last_line = <$fh>;
		chomp $last_line;
		while (my $this_line =<$fh>) {
			chomp $this_line;
			my $line = $last_line . $this_line;
			$line = sanitised_line($line);
			#print "$line\n" if $verbose;

			# Figure out what sort of press release this is
			if ( $line =~ /Public hospital daily update on Wuhan-related cases/ ) {
				$table = "HongKong";
				#print "\t$table\n";
			} 
			if ( $line =~ /Public hospitals heighten vigilance/ ) {
				$table = "Wuhan";
			} 
			if ( $line =~ /CHP closely monitors cluster/ ) {
				$table = "Wuhan";
			} 
			if ( $line =~ /ha[dev]+ admitted (.*) patients/ ) {
				$new_cases = digits_from_words($1);
			}
			if ( $line =~ /reported (.*?) (concerned )*patient cases/ ) {
				$total = digits_from_words($1);
			}
			if ( $line =~ /identified (.*?) cases/ ) {
				$total = digits_from_words($1);
			}
			if ( $line =~ /(.*?) concerned patient cases/ ) {
				$total = digits_from_words($1);
			}
			if ( $line =~ /December 31, 2019. (.*)/ ) {
				my $new_num = digits_from_words($1);
				if ( $discharged = 0 && $new_num > 0) {
					$discharged = $new_num;
				}
			}
			if ( $line =~ /([A-Za-z-]) of them have been discharged./ ) {
				my $new_num = digits_from_words($1);
				if ( $discharged = 0 && $new_num > 0) {
					$discharged = $new_num;
				}
			}

			$last_line = $this_line;
		}
		close $fh;
		if (defined($table)) {
			print "$file: $timestamp, $date, $pr_num, $table, $new_cases, $total, $discharged\n" if $verbose;
			my $result = dbdo($db, "INSERT INTO [$table] (Timestamp, Date, PR_num, new_cases, total, discharged) Values ($timestamp, $date, $pr_num, $new_cases, $total, $discharged);", $dbverb);
		}
	}
}
dbdo($db, "COMMIT", $dbverb);
$db.close();


## SUBS
sub sanitised_line {
	my $line = shift;
	# check if the line contains quoted disease name
	#print "Sanitising: $line\n" if $verbose;
	$line =~ s/\r//g;

	# Leading, trailing and multiple spaces
	$line =~ s/\s+/ /g;
	$line =~ s/\s+$//g;
	$line =~ s/^s+//g;
	
	#print "Sanitising: $line\n";	
	#Trailing commas
	$line =~ s/,+$//g;
	return $line;

}

sub digits_from_words {
	my $words = shift;
	my @words = split("[ -]", lc($words));
	my $digits = 0; # placeholder
	if ( $words =~ /[0-9]+/ ) {
		$digits = $words;
	}
	for my $word (@words) {
		if ( $word eq "one"  ) {$digits += 1;}
		if ( $word eq "two"  ) {$digits += 2;}
		if ( $word eq "three") {$digits += 3;}
		if ( $word eq "four" ) {$digits += 4;}
		if ( $word eq "five" ) {$digits += 5;}
		if ( $word eq "six"  ) {$digits += 6;}
		if ( $word eq "seven") {$digits += 7;}
		if ( $word eq "eight") {$digits += 8;}
		if ( $word eq "nine" ) {$digits += 9;}
		if ( $word eq "ten"  ) {$digits += 10;}
		if ( $word eq "eleven"  ) {$digits += 11;}
		if ( $word eq "twelve"  ) {$digits += 12;}
		if ( $word eq "thirteen") {$digits += 13;}
		if ( $word eq "twenty") {$digits += 20;}
		if ( $word eq "thirty") {$digits += 20;}
	}
	print "\tDFW:$words -> $digits\n" if $verbose;
	return $digits;
}
	
