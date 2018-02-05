#!/usr/bin/perl

use strict;
use DBI;

open my $ind, "../../data/StockNames/India/stocks.txt" or die "Cannot open indian stock list file: $!\n";
open my $us, "../../data/StockNames/US/stocks.txt" or die "Cannot open american stock list file: $!\n";

#download_indian();
download_us();
#download_earnings_cal();
#download_historical_earnings_statement();
load_to_db();

sub download_indian{
	print "Working on Indian stocks\n";
	while(<$ind>){
		my $stock = $_; chomp $stock;
		print "\t\tWorking on $stock now\n";	
		`./INDIA_getYTDStockData.py $stock`;	
	}
}

sub download_historical_earnings_statement{
	print "Working on downloading historical earnings\n";
	while(<$us>){
		my $symbol = $_; chomp $symbol;
		my $sym_file = $symbol."_quarterly_financial_data.csv";
		next if($symbol !~ /AAPL/);
		print $sym_file."\n";
		`cd /Users/VK/Desktop/UChicago/Capstone/data/Earnings/raw_data/ ; wget http://www.stockpup.com/data/$sym_file >>/dev/null 2>&1 `;
		convert_raw_data_to_quarterly_time_series($symbol, $sym_file);
	}	
}

sub convert_raw_data_to_quarterly_time_series{
	my ($symbol, $sym_file) = @_;
	my $raw_dir  = "/Users/VK/Desktop/UChicago/Capstone/data/Earnings/raw_data/";
	my $dest_dir = "/Users/VK/Desktop/UChicago/Capstone/data/Earnings/historical/";	

	my %sorted_by_time;

	open my $sym, "$raw_dir/$sym_file" or next;
	open my $write_sym, ">$dest_dir/$sym_file" or die "Cannot open destination directory for writing $symbol file:$!\n";

	while(<$sym>){
		my $line = $_; chomp $line;
		if($line =~ /Quarter/){
			$line =~ s/"//g;
			$line =~ s/Quarter end/Year,Quarter/;
			print $write_sym "$line\n";
		}else{
			my ($year, $qtr) = $line =~ /^(\d{4})-(\d{2})-/;
			if($qtr <= 3){
				$qtr = 1;
			}elsif(($qtr >3) and ($qtr <= 6)){
				$qtr = 2;
			}elsif(($qtr >6) and ($qtr <= 9)){
				$qtr = 3;
			}else{
				$qtr = 4;
			}
			$line =~ s/^.*?,/$year,$qtr,/;
			$sorted_by_time{$year.$qtr} = $line;
		}
	}

	foreach my $l(sort keys %sorted_by_time){
		print $write_sym "$sorted_by_time{$l}\n";
	}

}

sub download_us{
        print "Working on US stocks\n";
        while(<$us>){
                my $symbol = $_; chomp $symbol;

                my @dirs = ('1_day_5_years','5_mins_1_year');

                foreach my $dir(sort @dirs){
                        my $filename = "../../data/StockPrices/US/$dir/$symbol.csv";
                        my $new_filename = "../../data/StockPrices/US/$dir/$symbol.new.csv";
                        print $filename."\n";

                        if(-e $filename){
                                `rm $filename`; # remove existing data so that you can create new data
                        }

                        print "\t\tWorking on $symbol now\n";

                        if($dir =~ /^1/){
                                `./US_getYTDStockData.py $symbol`;
                        }else{
                                `./US_getIntraDay1YR.py $symbol`;
                        }
                        sleep 1;

                        open my $fh, $filename or next;
                        open my $wh, ">$new_filename" or die "Cannot find $new_filename for writing: $!\n";

                        while(<$fh>){
                                my $line = $_; chomp $line;
                                $line =~ s/^.*?,//g if($dir =~ /^1/);
                                $line =~ s/\"//g if($dir =~ /^1/);
                                $line = "TIMESTAMP".$line if($line =~ /CLOSE/);
                                print $wh "$line\n";
                        }
                        close $fh;
                        close $wh;

                        `rm $filename`; # remove unformatted file
                        `mv $new_filename $filename`; # create new formatted file
                }
        }
}

sub load_to_db{

	my $user = "root";
	my $pass ="K4Gayathri";
	my $host = "localhost";

	my $dbh = DBI->connect("dbi:mysql:INDIAN_STOCKS:$host",$user,$pass);
	my $us_dbh = DBI->connect("dbi:mysql:US_STOCKS:$host",$user,$pass);

	my @indian_files = glob("../../data/StockPrices/India/*csv");
	my @us_files = glob("../../data/StockPrices/US/1_day_5_years/*csv");
        
	my $delete_table = qq{delete from INDIAN_STOCKS.TickerVsPrice};
	my $delete_us_table = qq{delete from US_STOCKS.TickerVsPrice};

        my $sqlQuery = $dbh->prepare($delete_table); $sqlQuery->execute();
        my $us_sqlQuery = $dbh->prepare($delete_us_table); $us_sqlQuery->execute();

	foreach my $in_f(sort @indian_files){
        	my $load_table = qq{load data local infile \'$in_f\' into table INDIAN_STOCKS.TickerVsPrice fields terminated by ',' LINES TERMINATED BY \'\\n\' IGNORE 1 LINES};
        	$dbh->do($load_table);
	}

        foreach my $us_f(sort @us_files){
                my $load_table = qq{load data local infile \'$us_f\' into table US_STOCKS.TickerVsPrice fields terminated by ',' LINES TERMINATED BY \'\\n\'};
                $us_dbh->do($load_table);
        }

}
