<?php
        $array1 = array();
        $array2 = array();
        $array3 = array();

        include('../lib/connection.php');
        if(! dbconn ) {die('Could not connect ' );
        }

        $result1 = pg_query($dbconn, 'SELECT * FROM output_ranking;');
        while ($row1 = pg_fetch_row($result1)) {

                array_push ($array1, array($row1[0],$row1[1],$row1[2],$row1[3],$row1[4],$row1[5]));
        }
        $ans1 = json_encode($array1);

        $result2 = pg_query($dbconn, 'SELECT * FROM output_performance;');
        while ($row2 = pg_fetch_row($result2)) {
                                settype($row2[1], "double");
                                settype($row2[2], "double");
                                settype($row2[3], "double");
                                settype($row2[4], "double");
                                settype($row2[5], "double");
                array_push ($array2, array($row2[0],$row2[1],$row2[2],$row2[3],$row2[4],$row2[5]));
        }
        $ans2 = json_encode($array2);

        $result3 = pg_query($dbconn, 'SELECT * FROM output_short;');
        while ($row3 = pg_fetch_row($result3)) {
                                settype($row3[1], "double");
                                settype($row3[2], "double");
                                settype($row3[3], "double");
                                settype($row3[4], "double");
                                settype($row3[5], "double");
                array_push ($array3, array($row3[0],$row3[1],$row3[2],$row3[3],$row3[4],$row3[5]));
        }
        $ans3 = json_encode($array3);
?>

<html>

      <head>

        <!--Load the AJAX API-->

        <script type="text/javascript" src="https://www.google.com/jsapi"></script>

        <script type="text/javascript">

          google.load('visualization', '1.0', {'packages':['corechart','table']});

          google.setOnLoadCallback(drawChart);

          function drawChart() {

            var data = new google.visualization.DataTable();
            data.addColumn('string', 'Code');
            data.addColumn('string', 'D0');
            data.addColumn('string', 'D1');
            data.addColumn('string', 'D2');
            data.addColumn('string', 'D3');
            data.addColumn('string', 'D4');
            data.addRows(<?php echo "$ans1" ?>);


            var data2 = new google.visualization.DataTable();
            data2.addColumn('string', 'Date');
            data2.addColumn('number', 'top1');
            data2.addColumn('number', 'top2');
            data2.addColumn('number', 'top3');
            data2.addColumn('number', 'top4');
            data2.addColumn('number', 'top5');
            data2.addRows(<?php echo "$ans2" ?>);



            var data3 = new google.visualization.DataTable();
            data3.addColumn('string', 'Date');
            data3.addColumn('number', 's30');
            data3.addColumn('number', 's40');
            data3.addColumn('number', 's50');
            data3.addColumn('number', 's60');
            data3.addColumn('number', 'flag');
            data3.addRows(<?php echo "$ans3" ?>);



            // Set chart options

            var options = {'title':'Ranking List in last 5 Business Day',

                           'width':400,

                           'height':300};

            // Set chart options

            var options2 = {'title':'Equity Performance in Last 10 Business Day',

                           'width':400,

                           'height':300};

            // Set chart options

            var options3 = {'title':'Stopping Point in Last 10 Business Day',

                           'width':400,

                           'height':300};



            // Instantiate and draw our chart, passing in some options.

            var table = new google.visualization.Table(document.getElementById('table_div'));
            table.draw(data, {showRowNumber: false, width: 400, height: 300});

            var chart2 = new google.visualization.LineChart(document.getElementById('chart_div2'));
            chart2.draw(data2, options2);

            var chart3 = new google.visualization.LineChart(document.getElementById('chart_div3'));
            chart3.draw(data3, options3);



          }

        </script>

      </head>



      <body>

        <!--Divs that will hold the charts-->

        <div id="table_div"></div>

        <div id="chart_div2"></div>

        <div id="chart_div3"></div>

      </body>

    </html>



