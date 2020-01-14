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
use HTML::Entities;

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

dbdo($db, 'BEGIN', $dbverb);
for my $file (@files) {
	chomp $file;
	print "\n\nProcessing file: $file...\n";
	my ($timestamp, $date, $pr_num, $table, $new_cases, $total, $discharged) = process_file($file);
	if (defined($table)) {
		print "$file: $timestamp, $date, $pr_num, $table, $new_cases, $total, $discharged\n" if $verbose;
		my $result = dbdo($db, "INSERT INTO [$table] (Timestamp, Date, PR_num, new_cases, total, discharged) Values ($timestamp, $date, $pr_num, $new_cases, $total, $discharged);", $dbverb);
	}
	print "finished $file...\n\n";
	#sleep 2;}
}

dbdo($db, "COMMIT", $dbverb);
$db.close();

## SUBS
sub process_file {
	my $file = shift;
	if ($file =~ /P(\d{8})(\d+)\.htm/) {
		my $date = $1;
		my $pr_num = $2;
		my $timestamp = "$1$2";
		my $table;
		my $fields = "Timestamp, Date, pr_num, new_cases, stable, discharged";
		my ($new_cases, $total, $discharged) = (0, 0, 0);
		print "\tPF: $date:$pr_num\n" if $verbose;

		my @pr_body = `grep  \"^&nbsp;\" $datadir/$file`;
		my @pr_all = `lynx -dump $datadir/$file`;

		for my $para (@pr_body) {
			chomp $para;
			print "\t\tPARA:$para\n" if $verbose;
			$para = sanitise_text($para);
			print "\t\tPARA:$para\n" if $verbose;
			my @lines = split('\.', $para);
			for my $line (@lines){
				print "\t\tLINE:$line\n" if $verbose;

				# Figure out what sort of press release this is
				if ( $line =~ /[Hh]ospital[s]* ha[dve]+ admitted.*Wuhan.*/ ) {
					$table = "HongKong";
					#print  $text\n" if $verbose;
					#print "\t$table\n";
				}
				if ( $line =~ /Wuhan Municipal Health Commission/ ) {
					$table = "Wuhan";
				}
				if ( $line =~ /ha[dve]+ admitted (.*?) patient/ ) {
					$new_cases = max($new_cases, digits_from_words($1));
					print  "$line\n" if $verbose;
				}
				if ( $line =~ /reported (.*?) (concerned )*patient cases/ ) {
					$total = max($total, digits_from_words($1));
				}
				if ( $line =~ /, (.*) cases have been reported./) {
					$total = max($total, digits_from_words($1));
				}
				if ( $line =~ /identified (.*?) cases/ ) {
					$total = max($total, digits_from_words($1));
				}
				if ( $line =~ /(.*?) concerned patient cases/ ) {
					$total = max($total, digits_from_words($1));
				}
				if ( $line =~ /,(.*) have been discharged./ ) {
					$discharged = max($discharged, digits_from_words($1));
				}
				if (defined($table)) {
					print "\t$file: $timestamp, $date, $pr_num, $table, $new_cases, $total, $discharged\n" if $verbose;
				}
			}
		}
		return ($timestamp, $date, $pr_num, $table, $new_cases, $total, $discharged);
	} else {
		print "Input Arguments not recognised!\n";
	}
}

sub max {
	my $local_max = shift;
	while (my $next = shift) {
		if ($next > $local_max ) {
			$local_max = $next;
		}
	}
	return $local_max;
}

sub sanitise_text {
	my $text = shift;
	# check if th $text contains quoted disease name
	#print "Sanitising: $text\n" if $verbose;

	# These need to be done in a particular order
	# Handle HTML entities
	$text =~ s/\&nbsp;/ /g; # decode_entities $text);
	$text =~ s/\&#39;/'/g; # decode_entities $text);
	$text =~ s/\<[\/a-z]+.*?\>//g; # HTML Tags

	# # Leading, trailing and multiple spaces
 	$text =~ s/\s+$//g;
 	$text =~ s/^s+//g;
 	$text =~ s/\s+/ /g;

	# # Remove confusing time references
 	$text =~ s/ 24 hours//g;
 	$text =~ s/ 14 days//g;

	# #print "Sanitising: $text\n";
	# #Trailing commas
 	$text =~ s/,+$//g;
	
	return $text;
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
