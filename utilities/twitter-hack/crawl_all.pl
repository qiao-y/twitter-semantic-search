#!/usr/bin/perl -w 

use strict;
use warnings;
use File::Basename;
use Sys::Hostname;

#sendemail -f  qiaoyu.yu@gmail.com -t Twitter-semantic-search@lists.cs.columbia.edu  -u "Test: sent from bash" -m "test" -s smtp.gmail.com:587 -o tls=yes -xu  qiaoyu.yu -xp dummy

# $_[0]: current file name
# $_[1]: total file count
# $_[2]: done file count
sub my_sendmail($$$){
        my $from = 'dummy@gmail.com';
        my $to = 'Twitter-semantic-search@lists.cs.columbia.edu';
        my $host = hostname;
        my $subject = "$host: Finished downloading $_[2]/$_[1]";
        if ($_[1] == $_[2]){
		$subject = "$host: ALL DONE!!! Finished downloading $_[2]/$_[1]";}
	my $body = "Hostname: $host\nTotal file count: $_[1]\nDownloaded: $_[2]\nCurrent File Name: $_[0]\nTime: ". localtime;
        my $cmd = "sendemail -f $from -t $to -u \"$subject\" -m \"$body\" -s smtp.gmail.com:587 -o tls=yes -xu  dummy -xp dummy";
        system("$cmd &");
}

my $count = 1;
my @all_files = <input/*.dat>;
my $file_count = scalar(@all_files);
foreach my $input_file (@all_files){
	my $basename = basename($input_file,".dat");
	my $output_file = "output/$basename.out";
	my $log_file = "log/$basename.log";
#	print "input file: $input_file, output file: $output_file \n";
	my $now_string = localtime;
	print "[$now_string]: /home/qiaoyu/twitter-hack/run.sh edu.columbia.watson.twitter.TwitterCrawler $input_file $output_file 2>&1 > $log_file\n";	
	system("/home/qiaoyu/twitter-hack/run.sh edu.columbia.watson.twitter.TwitterCrawler $input_file $output_file 2>&1 > $log_file\n");
	$now_string = localtime;
	print "[$now_string]: $input_file done...\n";
	if ($count % 50 == 0){
		my_sendmail($basename,$file_count,$count);
	}
	$count++;
}
my_sendmail("",$file_count,$file_count);

 
