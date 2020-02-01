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

# For development purposes, fail on warnings
$SIG{__WARN__} = sub {
	if ($_[0] =~ /^Use of uninitialized value/) {
		require Carp;
		Carp::cluck();
		die;
	} else {
		warn @_;
	}
};	

my $verbose = 1;
my $dbverb = 0;
my $firstrun = 1;
my $datadir = "./01_download_data";
my @files = `ls $datadir/nid*.csv`;
my $db = dbconnect("notifiable_infections_diseases.sqlite", "", "") or die $DBI::errstr;
my @months = qw/Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec Total/;
my $columns = "Year, " . join(", ", @months);
my $coldefs = "Year INTEGER UNIQUE PRIMARY KEY, ".join( ", ", map { $_ ." Integer"} @months);
my %diseases;
my %disease_names;
my %disease_refs;

print "@months\n";
print "$columns\n";
print "$coldefs\n";

if ( $firstrun) {
	#Make Tables
	my $result = drop_all_tables($db, "", $dbverb);

	my %tables = (
		"diseases" => "ref Text, name Text UNIQUE Primary Key",
		"months"   => "Timestamp Integer UNIQUE PRIMARY KEY, Year Integer, Month Text",
	);
	my $tables = make_db($db, \%tables, $dbverb);
	print "$tables tables initialised.\n";
	open (my $fh, "<", "$datadir/diseases.srt") or die "Can't open the disease abbreviations! $!";
	while (my $line = <$fh>) {
		chomp $line;
		$line =~ s/\"//g;
		my ($short_name, $long_name) = split(",", $line);
		$disease_names{$short_name} = $long_name;
		$disease_refs{$long_name} = $short_name;
		print "$long_name, $short_name\n" if $verbose;
	}
	close $fh;
}

print "Processing @files...\n" if $verbose;
for my $file (@files) {
	chomp $file;
	print "Processing file: $file...\n";

	if ($file =~
		/nid([0-9]+)en\.csv$/) {
		my $year = $1;
		dbdo($db, "BEGIN", $dbverb);

		open (my $fh, "<", "$file") or die "Can't open $file. $!";
		while (my $line =<$fh>) {
			chomp $line;
			$line = sanitised_line($line);
			#print hex_from_string($line, 2) . "\n" if $verbose;
			my @components = split ",", $line;

			if ( $components[0] eq "Disease") {
				# This is just the header row
			} else {
				my $disease = shift @components;
				$disease = join '', map { ucfirst lc $_ } split /(\s+)/, $disease;
				$diseases{$disease}++;
				my $disease_ref = $disease_refs{$disease};

				print "Disease: $disease ($diseases{$disease} - $disease_ref )...\n" if $verbose;
				if ( $diseases{$disease} == 1 ) {
					my $result = dbdo($db, "CREATE TABLE if not exists [$disease_ref] ($coldefs);", $dbverb);
					dbdo($db, "INSERT OR IGNORE into [Diseases] (Ref, Name) Values (\"$disease_ref\", \"$disease\");", $dbverb);
				}
				my $values = "$year, " . join(", ", map{ "\"$_\""} @components);
				my $result = dbdo($db, "INSERT OR IGNORE INTO [$disease_ref] ($columns) Values ($values);", $dbverb);
				if ($disease =~ /disease/ ) {
					print "BARF: $line\n";
					exit;
				}
			}

		}
		close $fh;
		dbdo($db, "COMMIT", $dbverb);
	}
	#exit;
}
sub sanitised_line {
	my $line = shift;
	# check if the line contains quoted disease name
	$line =~ s/\r//g;
	$line =~ tr/\x{EF}\x{BB}\x{BF}//d;
	$line =~ s/'//g;
	$line =~ s/\s+$//g;
	$line =~ s/^\s+//g;
	
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
	$line =~ s/\s+,/,/g;
	return $line;

}
sub hex_from_string {
	my $string = shift;
	my $width = shift;
	my $format = "%" . sprintf( "%02d", $width) . "X";
	my $hex_string = '';
	my @chars = split( "", $string);
	while (my $char = shift @chars) {
		my $code = ord($char);
		$hex_string .= sprintf( $format, $code) . ' ';
	}
	return $hex_string;
}

