############################################################################################
## MoveFile
##
## 14-FEB-2013
## Greg Holliday
## AssetPoint
##
############################################################################################
## This script does the following 
##  1) connect to the Oracle database, and stop the current log.
##  2) Moveve all the log files created prior current date to a staging folder.
##  3) FTP zipped files to DR server
##  4) If FTP was successful, move zip files to archive folder
##  5) Delete archive files older that XX days
############################################################################################

#!/usr/bin/perl -w
############################################################################################
# Modules
############################################################################################
use strict;
use Net::FTP; #FTP module
use File::Copy; #File copy
use File::Basename;
use Archive::Zip; #Zip module
use Config::Simple; #For reading INI file
use Time::localtime;
use Date::Calc qw(Delta_DHMS);
use Scalar::Util qw(looks_like_number);
use DBI;
use Fcntl;
use Mail::Sender;

############################################################################################
# Variable definition
############################################################################################
my $ini;
my $ini_path = 'T:\\Utils\\MaintenanceScripts\\DR_FTP\\config.ini';
#my $ini_path = 'C:\\Utils\\MaintenanceScripts\\DR_FTP\\config.ini';
my %config;
my $file;
my $st;
my $day;
my $month;
my $year;
my $min;
my $hour;
my $sec;
my $cmin=0;
my $csec=0;
my $chour=0;
my $cday=0;
my $cmonth=0;
my $cyear=0;
my $local;
my @cts;
my @fts;
my @diff;
my $temp;
my $logfile;
my $newfile;
my $orgfile;
my $errfnd = 0;
my $zip;
my $name;
my $ftp_success=1;
my $log_ind = "T";
my $runfile;
my $filecnt;
my $zipcnt;

############################################################################################
# END Variable definitions
############################################################################################

############################################################################################
##    Main processing
############################################################################################

############################################################################################
# get current date for error log file name and start date
############################################################################################
$local = (localtime);
$csec = $local->[0];
if ($csec < 10){
	$csec = '0'.$csec;
}
$cmin = $local->[1];
if ($cmin < 10){
	$cmin = '0'.$cmin;
}
$chour = $local->[2];
if ($chour < 10){
	$chour = '0'.$chour;
}
$cday = $local->[3];
$cmonth = $local->[4] + 1;
$cyear = $local->[5] + 1900;
if ($cmonth < 10){
	$cmonth = '0'.$cmonth;
}
if ($cday < 10){
	$cday = '0'.$cday;
}

############################################################################################
#read configuration file
############################################################################################
print "Reading config file\n";
$ini = new Config::Simple($ini_path);
%config = $ini->vars();

my $log_path = $config{'Error.logpath'};
my $log_name = $config{'Error.logname'};
my $log_ext = $config{'Error.logext'};

my $staging = $config{'File Locations.staging'};
my $archive = $config{'File Locations.archive'};
my $original = $config{'File Locations.original'};
my $ziploc = $config{'File Locations.zip'};
my $hold = $config{'File Locations.hold'};
my $serverdir = $config{'File Locations.serverdir'};
my $age = $config{'File Locations.age'};
$age = $age*60*60*24; #Convert age from days to seconds

my $dbname = $config{'Database.dbname'};
my $dbuser = $config{'Database.dbuser'};
my $dbpwd = $config{'Database.dbpwd'};
my $dbusezz = $config{'Database.dbusezz'};

my $ftpsrv = $config{'FTP.server'};
my $ftpusr = $config{'FTP.user'};
my $ftppwd = $config{'FTP.password'};

my $lryear = $config{'Date.lryear'};
my $lrmonth = $config{'Date.lrmonth'};
my $lrday = $config{'Date.lrday'};
my $lrhour = $config{'Date.lrhour'};
my $lrmin = $config{'Date.lrmin'};
my $lrsec = $config{'Date.lrsec'};

my $mail_from = $config{'Mail.from'};
my $mail_to = $config{'Mail.to'};
my $mail_smtp = $config{'Mail.smtp'};
my $mail_cc = $config{'Mail.cc'};

if ($lryear eq ''){
	$lryear = $cyear;
}
if ($lrmonth eq ''){
	$lrmonth = $cmonth;
}
if ($lrday eq ''){
	$lrday = $cday;
}
if ($lrhour eq ''){
	$lrhour = $chour;
}
if ($lrmin eq ''){
	$lrmin = $cmin;
}
if ($lrsec eq ''){
	$lrsec = $csec;
}
my @lastrun = ($lryear,$lrmonth,$lrday,$lrhour,$lrmin,$lrsec);

############################################################################################
#END - read configuration file
############################################################################################

############################################################################################
# open error log file
############################################################################################
$logfile = $log_path.'\\'.$log_name.$cmonth.$cday.$cyear.".".$log_ext; #This just sets the error log path and name. It is opened in err()
$runfile = "$0.log";
open(RUN,">>$runfile");
print RUN "--------------------START SESSION----------------------------------\n";
print RUN "DR-Recovery started on $cmonth/$cday/$cyear at $chour:$cmin:$csec\n";

################################################
# This is used for debugging.                  #
# It will be used to collect the files copied  #
# in the current run.                          #
################################################
my $lastrun = "$0.dbg";
open(LAST,">>$lastrun");
print LAST "-------------START SESSION $cmonth/$cday/$cyear $chour:$cmin:$csec-------------\n";

############################################################################################
#Connect to DB and stop log
############################################################################################
print "Connecting to database\n";
print LAST "Connecting to database\n";
if($dbusezz == 1){
$dbuser = 'zz'.$dbuser;   #put zz in front of user id
$dbpwd = reverse($dbpwd); #reverse password
my $first = substr($dbpwd,0,1);
if(looks_like_number($first)){
	 $dbpwd = 'z'.$dbpwd; #put z in front of password if the first character is a z
}
}
my $dbh = DBI->connect("dbi:Oracle:$dbname",$dbuser,$dbpwd)
	or err("DR-Recovery:MoveFiles","Database Error - Cannot connect to database: $DBI::errstr\n",2);

my $sql = "ALTER SYSTEM SWITCH LOGFILE";

$dbh->do($sql) or err("DR-Recovery:MoveFiles","Database Error - Error switching log files:  $DBI::errstr\n",2);
print "Switching DB log. Waiting...\n";
sleep(45); #wait for 45 seconds


############################################################################################
# get current date after SWITCH for processing file copy/ftp
############################################################################################
$local = (localtime);
$csec = $local->[0];
if ($csec < 10){
	$csec = '0'.$csec;
}
$cmin = $local->[1];
if ($cmin < 10){
	$cmin = '0'.$cmin;
}
$chour = $local->[2];
if ($chour < 10){
	$chour = '0'.$chour;
}
$cday = $local->[3];
$cmonth = $local->[4] + 1;
$cyear = $local->[5] + 1900;
if ($cmonth < 10){
	$cmonth = '0'.$cmonth;
}
if ($cday < 10){
	$cday = '0'.$cday;
}
@cts = ($cyear,$cmonth,$cday,$chour,$cmin,$csec);
$ini->param('Date.lryear',$cyear);
$ini->param('Date.lrmonth',$cmonth);
$ini->param('Date.lrday',$cday);
$ini->param('Date.lrhour',$chour);
$ini->param('Date.lrmin',$cmin);
$ini->param('Date.lrsec',$csec);
$ini->save();
############################################################################################
# END-get current date
############################################################################################

############################################################################################
# Get list of files to transfer from DB
############################################################################################
print "Getting list of files from database\n";
my $lastrun = "$lrday-$lrmonth-$lryear $lrhour:$lrmin";
my $currentrun = "$cday-$cmonth-$cyear $chour:$cmin";

$sql = "select name, to_char(completion_time, 'DD-MM-YYYY HH24:MI') ".
		"from v\$archived_log ".
		"where UPPER(name) like 'U%' ".
		"and completion_time between to_date('$lastrun', 'DD-MM-YYYY HH24:MI') ".
		"and TO_DATE('$currentrun' ,'DD-MM-YYYY HH24:MI')";

my $sth = $dbh->prepare($sql)
	or err("DR-Recovery:MoveFiles","Database Error - Can not get files from DB: $DBI::errstr\n",2); ##prepare the SQL
$sth->execute()
	or err("DR-Recovery:MoveFiles","Database Error - Can not get files from DB: $DBI::errstr\n",2); ##execute the SQL

my $array_ref = $sth->fetchall_arrayref(); ##fetch rows into array
my @files;

foreach my $row(@$array_ref){
	my ($filename,$filedate) = @$row;
	push(@files, $filename);
}
$dbh->disconnect();  #disconnect from the database

############################################################################################
## Move log files from original location to 
## the staging folder if they were created
## before the current system date
############################################################################################
my $temp2;
$filecnt = 0; #make sure the file count is reset

print "Moving files to staging area\n";
print LAST "Moving files to staging area\n";
my ($base,$ext,$dir);

foreach $file(@files){
	($base,$dir,$ext) = fileparse($file);
	$newfile = $staging."\\".$base.$ext;
	#print "Moving $file to $newfile\n";
	copy($file,$newfile)
		 or err("DR-Recovery:MoveFiles","Error trying to copy file from $file to $newfile\n",1);
	print LAST "Copied $orgfile TO $newfile\n";
	$filecnt++; #increment the file count after copy	
}

############################################################################################
##zip and ftp files to DR-PROD
############################################################################################
print "Zipping files\n";
print LAST "Zipping files\n";

$zipcnt = 0; #Make sure zip file count is reset
opendir (STG, $staging) or err("DR-Recovery:MoveFiles","Could not open staging folder $staging\n",2);
my @zipfiles=readdir(STG);
closedir(STG);
foreach $file (@zipfiles){
   next if $file =~ /^\.\.?$/;  # skip . and ..
   $orgfile = $staging."\\".$file;
   $zip = Archive::Zip->new();
   $zip->addFile($orgfile);
   $name = basename($orgfile);
   $name = $ziploc."\\".$cyear.$cmonth.$cday."_".$chour.$cmin."_".$name.".zip";
   if ($zip->writeToFileNamed($name) != 0){
       err("DR-Recovery:MoveFiles","Error creating ZIP file $name\n",1);
		$temp=$hold."\\".$file;
		copy($orgfile,$temp) or err("DR-Recovery:MoveFiles","Error trying to move file from $name to $newfile\n",1);
   }else{
		$zipcnt++;
		unlink($orgfile);
	}
}

############################################################################################
#FTP files
############################################################################################
print "FTP files\n";
print LAST "FTP files\n";

if($filecnt != $zipcnt){
	err("DR-Recovery:MoveFiles","Mismatch between the number of LOG files and ZIP files.\n",2);
}else{
	print "Sending files via FTP\n";
	my $loc_size;
	my $ftp_size;
	my $return;
	my $ftp = Net::FTP->new($ftpsrv, Debug => 0)
		or err("DR-Recovery:MoveFiles","Error creating FTP instance\n",2);
	$ftp->login($ftpusr,$ftppwd)
		or err("DR-Recovery:MoveFiles","Error connecting to FTP server\n",2);
	$ftp->binary();
	
	opendir (SND, $ziploc) or err("DR-Recovery:MoveFiles","Could not open staging folder $staging\n",2);
	my @ftpfiles = readdir(SND);
	closedir(SND);
	foreach $file (@ftpfiles){
		next if $file =~ /^\.\.?$/;  # skip . and ..
		$newfile = $archive."\\".$file;
		$name = $ziploc."\\".$file;
		$ftp->cwd($serverdir); #change directory for FTP
		$return = $ftp->put($name);  #send file to FTP server 
		$ftp_size = $ftp->size($return); #get the size of the file that was FTPed ($temp = filename without directory) 
		$loc_size = -s $name;
		if($ftp_size >= $loc_size){
			move($name,$newfile)
				or err("DR-Recovery:MoveFiles","Error trying to move file from $name to $newfile\n",1); #move the zip file to the archive folder
			unlink($name); #delete the original file
		}else{
			err("DR-Recovery:MoveFiles","Issue sending $name via FTP. File moved to 'hold' directory.\n",1);
			move($name, $hold."\\".$temp)
				or err("DR-Recovery:MoveFiles","Error trying to move file from $name to $hold\n",1);#move the zip file to the hold folder
			unlink($name); #delete the original file
		}
	}
	$ftp->quit; #Close FTP connection
}
############################################################################################
# Delete archive files older than ... days
############################################################################################
#opendir (ARC, $archive) or logerror("Can't open archive folder");
#my $now = time();
#while ($file = readdir ARC){
#	$file = $archive."\\".$file;
#	$st = localtime((stat ($file))[9]); #get the modified time
#	if ($now-$st > $age){
#		unlink($file);
#	}
#}

############################################################################################
# get current date for end date
############################################################################################
my($dsec,$dmin,$dhour,$dday,$dmonth,$dyear);
$local = (localtime);
$dsec = $local->[0];
if ($dsec < 10){
	$dsec = '0'.$dsec;
}
$dmin = $local->[1];
if ($dmin < 10){
	$dmin = '0'.$dmin;
}
$dhour = $local->[2];
if ($dhour < 10){
	$dhour = '0'.$dhour;
}
$dday = $local->[3];
$dmonth = $local->[4] + 1;
$dyear = $local->[5] + 1900;
if ($dmonth < 10){
	$dmonth = '0'.$dmonth;
}
if ($dday < 10){
	$dday = '0'.$dday;
}

if ($errfnd == 1) {
	print RUN "DR-Recovery completed with ERRORS on $dmonth/$dday/$dyear at $dhour:$dmin:$dsec\n";	
	email("DR-Recovery:MoveFile.pl completed with ERRORS");
}else{
	##finished without errors, write the last run time to the INI file.
	$ini->param('Date.lryear',$cyear);
	$ini->param('Date.lrmonth',$cmonth);
	$ini->param('Date.lrday',$cday);
	$ini->param('Date.lrhour',$chour);
	$ini->param('Date.lrmin',$cmin);
	$ini->param('Date.lrsec',$csec);
	$ini->save();	
 	print RUN "DR-Recovery completed SUCCESSFULLY on $dmonth/$dday/$dyear at $dhour:$dmin:$dsec\n";
	#email("DR-Recovery:MoveFile.pl completed with SUCCESSFULLY");
}
print RUN "---------------------END SESSION-----------------------------------\n";
print LAST "--------------END SESSION $cmonth/$cday/$cyear $chour:$cmin:$csec--------------\n";
close(RUN);
close(LAST);
print "DONE\n";
exit;  #Close Perl script

#########################################################
##    Sub routines
#########################################################
############################# # Error handling subroutine ############################# 
sub err{    
    my @args;                 ## Arguments passed in    
    my $subject;              ## Error Message Subject    
    my $message;              ## Error Message Text    
    my $severity;             ## Error Severity    
    my $sender;               ## Mail Sender    
    my $smtp;                 ## Mail SMTP
    my $to;
    my $from;
    my $cc;
    my $err=0;                ## Internal Error Notifier
    my $log_info;
    my $body;
    
    @args = @_;                                   ## Get Arguements    
    $subject  = $args[0];                         ## Error Subject    
    $message  = $args[1];                         ## Error Message    
    $severity = $args[2];                         ## Error Severity
    $smtp = $mail_smtp;
    $to = $mail_to;
    $cc = $mail_cc;
    $from = $mail_from;
    
    
    ##Write all errors to log file
    $log_info = $log_info.$cmonth."/".$cday."/".$cyear." ".$chour.":".$cmin." - $message\n";   
#    sysopen(LOGFILE,$logfile, O_WRONLY|O_CREAT|O_APPEND)           || ($err=1); 
    open(LOGFILE,">>$logfile") || ($err=1);
    if($err==1){        
            $log_info = "$log_info".$cmonth."/".$cday."/".$cyear." ".$chour.":".$cmin." - Error opening logfile $logfile during ".                    
                    "processing error $subject.\n.$message\n";        
            $message  = $log_info;        
            $subject  = "Error Opening Log File"; 
    }else{        
                print LOGFILE $log_info;        
                close LOGFILE;     
    }
    $errfnd = 1;

    ##If the error severity is 2 or greater, then send an email and kill the program
    if($severity >= 2){
        $sender = new Mail::Sender
            {smtp => $smtp, from => $from} || die "Email Error - 1";
        $sender->MailFile({to => $to,
                           cc => $cc,
                           subject => $subject,
                           msg => $message,
						   file => $logfile}) || die "Email Error - 2";
        
        die "Ending due to error";
    }
}

###########################Email when done#################################
sub email{
    my $sender;
    my @args = @_;
    #my $err = $args[0];
    my $subject = $args[0];
    my $message;
    my $smtp = $mail_smtp;
    my $to = $mail_to;
    my $from = $mail_from;
    my $cc = $mail_cc;    

    $sender = new Mail::Sender
        {smtp => $smtp, from => $from};
    
    #$subject = "DR-Recovery MoveFile.pl Completed Wiith Errors";
    #$message = "One or more errors occurred during the execution of MoveFile.pl.\n".
    #                "Please see attached log file for details.\n";
    $sender->MailFile({to => $to,
                           cc => $cc,
                           subject => $subject,
                           msg => $subject,
						   file => $logfile});                      
}						   
