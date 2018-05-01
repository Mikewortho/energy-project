// Global variables for the parameters to the graphing code
var selectedState = "";
var startDate = 0;
var endDate = 0;
var timePrecision = "";
var parseParameters = {};
var ba = ""

function parseStateColorsAndUpdate() {
    Papa.parse("../../gen_data/stateColours.csv", {
        download: true,
        complete: function(results) {
            var resultsData = results.data
            printMap(resultsData)
        }
    })
}

$(document).ready(function() {
    parseStateColorsAndUpdate()
    $(function(){
      $("#radioButtonContainer").load("radioButtons.html"); // As radio buttons file is very large, this was kept out of index.html for readability
    });

    $( function() { // Create datepickers for our two input boxes
        $( "#startDate").datepicker({
            changeMonth: true,
            changeYear: true,
            dateFormat: 'dd/mm/yy'});
    });

    $( function() {
        $( "#endDate").datepicker({
            changeMonth: true,
            changeYear: true,
            dateFormat: 'dd/mm/yy'});
    });
});

function printMap(data)
{
    // Array to object.
    var oldClass = ""
    var myStyles = {}

    for (var i = 1; i < data.length; i++) {
        myStyles[data[i][0]] = {fill: workOutValues(data[i][1])};
    }

    $('#map').usmap({
        stateStyles: {fill: '#5F5F5F'}, // Colour all states blue
        stateSpecificStyles: myStyles,  // Colour affected states according to their precision

        click: function(event, data) {
            $('#clicked-state').text('You have selected: ' + data.name);
            selectedState = data.name;
            document.getElementById("chart").style.display = "none"
            document.getElementById("statsContainer").style.display = "none"

            if(oldClass != "") {
                document.getElementById(oldClass).style.display = "none"
            }

            document.getElementById(data.name).style.display = "block" // Show the html for the checkboxes for the BAs
            window.scrollTo(0,document.body.scrollHeight);

            oldClass = data.name;
            console.log(data.name);
        },
    });
};

function workOutValues(percent)
{   // Create RGB values for the precision of each state its' forecast is with the actual
    percent = percent/100
    if(percent<=-0.5)
    {
        if(255-(-percent-0.5)*510<16)
        {
            return "#"+Math.round(255).toString(16)+"0"+Math.round(255-(-percent-0.5)*510).toString(16)+"0"+Math.round(0).toString(16)
        }
        else
        {
            return "#"+Math.round(255).toString(16)+Math.round(255-(-percent-0.5)*510).toString(16)+"0"+Math.round(0).toString(16)
        }
    }
    if(percent<=0)
    {
        if((-percent)*510<16)
        {
            return "#0"+Math.round((-percent)*510).toString(16)+Math.round(255).toString(16)+"0"+Math.round(0).toString(16)
        }
        else
        {
            return "#"+Math.round((-percent)*510).toString(16)+Math.round(255).toString(16)+"0"+Math.round(0).toString(16)
        }
    }
    if(percent<=0.5)
    {
        if((percent)*510<16)
        {
            return "#0"+Math.round(0).toString(16)+Math.round(255).toString(16)+"0"+Math.round((percent)*510).toString(16)
        }
        else
        {
            return "#0"+Math.round(0).toString(16)+Math.round(255).toString(16)+Math.round((percent)*510).toString(16)
        }
    }
    if(percent<=1)
    {
        if(255-(percent-0.5)*510<16)
        {
            return "#0"+Math.round(0).toString(16)+"0"+Math.round(255-(percent-0.5)*510).toString(16)+Math.round(255).toString(16)
        }
        else
        {
            return "#0"+Math.round(0).toString(16)+Math.round(255-(percent-0.5)*510).toString(16)+Math.round(255).toString(16)
        }
    }
}

// Get data from button presses
$('.button').click(function() {
    startDate = $('#startDate').val();      // Get dates from input boxes
    endDate = $('#endDate').val();
    timePrecision = $(this).val();          // take precision from button clicked
    ba = $("input[name='myRadio']:checked").val();
    parseParameters = {                         // create JSON for graph parameters
        'startDate' : startDate,
        'endDate' : endDate,
        'state' : selectedState,
        'timePrecision' : timePrecision,
        'BA' : ba
    }

    parseData(createGraph, parseParameters);    // Create graph using parameters
    parseStats(parseParameters);
    $('#statsTable').replaceWith('<table id="statsTable"><tr><th>Prediction</th><th>  R2 Score  </th><th>  Spearman Correlation  </th><th>  Pearson Correlation  </th><tr></table>')
    document.getElementById('statsContainer').style.display = "block"; //

    // updateAndDisplayStatsSidebar();      // Update the stats table
});

$(document).on('change',"input[name='myRadio']:radio",function(){                  // When radion button selected
    ba = $("input[name='myRadio']:checked").val();                                 // Get the right BA
    document.getElementById('dateContainer').style.display = "block";              // Show the date selection
    document.getElementById('selectionButtonContainer').style.display = "block";   // Show the radio buttons
    window.scrollTo(0,document.body.scrollHeight);                                 // Scroll to bottom of page
    console.log(ba);
});
