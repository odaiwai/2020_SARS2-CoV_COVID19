#!/usr/bin/env perl 
#===============================================================================
#
#         FILE: process_nid_data.pl
#        USAGE: ./process_nid_data.pl  
#  DESCRIPTION: Script to  parse the historic NID files and build the historic
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
my $dbverb = 0;
my $firstrun = 1;
my $datadir = "../01_download_data";
my @files = `ls $datadir`;
my $db = dbconnect("notifiable_infections_diseases.sqlite", "", "") or die $DBI::errstr;
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

	my %tables = (
		"diseases" => "name Text UNIQUE Primary Key",
		"months"   => "Timestamp Integer UNIQUE PRIMARY KEY, Year Integer, Month Text",
	);
	my $tables = make_db($db, \%tables, $dbverb);
	print "$tables tables initialised.\n";
}

print "Processing @files...\n" if $verbose;
for my $file (@files) {
	chomp $file;
	print "Processing file: $file...\n";

	if ($file =~ /nid([0-9]+)en\.csv$/) {
		my $year = $1;
		dbdo($db, "BEGIN", $dbverb);

		open (my $fh, "<", "$datadir/$file") or die "Can't open $datadir/$file. $!";
		while (my $line =<$fh>) {
			chomp $line;

			$line = sanitised_line($line);
			my @components = split ",", $line;
			if ( $components[0] eq "Disease") {
				# This is just the header row
			} else {
				my $disease = shift @components;
				$disease = join '', map { ucfirst lc $_ } split /(\s+)/, $disease;
				$diseases{$disease}++;
				print "Disease: $disease ($diseases{$disease})...\n" if $verbose;
				if ( $diseases{$disease} == 1 ) {
					my $result = dbdo($db, "CREATE TABLE [$disease] ($coldefs);", $dbverb);
					dbdo($db, "INSERT into [Diseases] (Name) Values (\"$disease\");", $dbverb);
				}
				my $values = "$year, " . join(", ", map{ "\"$_\""} @components);
				my $result = dbdo($db, 
					"INSERT OR IGNORE INTO [$disease] ($columns) Values ($values);", 
					$dbverb);
			}

		}
		close $fh;
		dbdo($db, "COMMIT", $dbverb);
	}
}
sub sanitised_line {
	my $line = shift;
	# check if the line contains quoted disease name
	$line =~ s/\r//g;
	$line =~ s/\s+$//g;
	
	#print "Sanitising: $line\n";
	if ( $line =~ /^\"(Influenza A\(H5\).+)\"/) {
		$line =~ s/\".*\"/Influenza A\(H5; H7; H9\)/g;
	}
	if ( $line =~ /^\"(Influenza A\(H2\).+)\"/) {
		$line =~ s/\".*\"/Influenza A\(H2; H5; H7; H9\)/g;
	}
	#print "Sanitising: $line\n";	
	#Trailing commas
	$line =~ s/,+$//g;
	return $line;

}
	
