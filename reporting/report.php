<html>
<?php
date_default_timezone_set('America/Los_Angeles');
$month 			= $_GET['month'];
$year 			= $_GET['year'];
$username 		= $_GET['username'];
$name 			= urldecode($_GET['name']);
$days_in_month 	= cal_days_in_month(CAL_GREGORIAN, $month, $year);
$date_string 	= "M d, Y";
$from			= date($date_string, mktime(0, 0, 0, $month, 1, $year));
$to 			= date($date_string, mktime(0, 0, 0, $month, $days_in_month, $year));
$file			= "data/$year/$month/$username/dpu.txt";
?> 
<head>
<!-- Turns caching off -->
<meta http-equiv="cache-control" content="max-age=0" />
<meta http-equiv="cache-control" content="no-cache" />
<meta http-equiv="expires" content="0" />
<meta http-equiv="expires" content="Tue, 01 Jan 1980 1:00:00 GMT" />
<meta http-equiv="pragma" content="no-cache" />
<!-- Loads fonts to match datasift.com site -->
<link href='//fonts.googleapis.com/css?family=Open+Sans:300,400,600,700' rel='stylesheet' type='text/css'>
<link href='//fonts.googleapis.com/css?family=Arvo' rel='stylesheet' type='text/css'> 
<style>
	body, p, h3 { font-family: "Open Sans", Helvetica, Arial, sans-serif; }
	hr { 	height: 12px;
			border: 0;
    		box-shadow: inset 0 12px 12px -12px rgba(0,0,0,0.5);
		}
</style>
</head>
<body>
<div style="margin-left:auto; margin-right:auto; width:1000px; font-family:arial"> 
<img src="logo.png" /><br/><br />
<p>
<strong>Prepared for: </strong><?php echo $name ?><br/>
<strong>Reporting Period: </strong><?php echo $from." - ".$to ?><br/>
<strong>DPU Usage: </strong><?php readfile($file); ?><br/>
</p>
</div>
<div style="margin-left:auto; margin-right:auto; width:1000px">
<h3>DPU Usage by Day</h3><hr>
<iframe id="dpus" src="dpus.html?month=<?php echo $month; ?>&year=<?php echo $year; ?>&username=<?php echo $username; ?>" height="450" width="1000" frameborder="0" scrolling="no"></iframe>
</div>
<div style="margin-left:auto; margin-right:auto; width:1000px">
<h3>License Usage by Day</h3><hr>
<iframe id="license" src="license.html?month=<?php echo $month; ?>&year=<?php echo $year; ?>&username=<?php echo $username; ?>" height="450" width="1000" frameborder="0" scrolling="no"></iframe>
</div>

<!-- Spacing for printing -->
<br/><br/><br/>

<div style="margin-left:auto; margin-right:auto; width:1000px">
<h3>Month-over-Month (Last 6 Months)</h3><hr>
<iframe id="license" src="month_over_month.html?month=<?php echo $month; ?>&year=<?php echo $year; ?>&username=<?php echo $username; ?>" height="450" width="1000" frameborder="0" scrolling="no"></iframe>
</div>
<div style="margin-left:auto; margin-right:auto; width:1000px">
	<h3>Licenses Breakdown</h3><hr>

	<div style="width: 100%; display: table;">
    <div style="display: table-row">
        <div style="width: 500px; display: table-cell; margin-bottom:-200px"><p style="padding-left:120px">By Cost</p> <iframe id="sources" src="sources_by_cost.html?month=<?php echo $month; ?>&year=<?php echo $year; ?>&username=<?php echo $username; ?>" height="500" width="500" frameborder="0" scrolling="no"></iframe> </div>
        <div style="display: table-cell;"><p style="padding-left:100px">By Volume</p> <iframe id="sources" src="sources_by_volume.html?month=<?php echo $month; ?>&year=<?php echo $year; ?>&username=<?php echo $username; ?>" height="500" width="500" frameborder="0" scrolling="no"></iframe> </div>
    </div>
    
</div>
</div>
</body> 
</html> 
