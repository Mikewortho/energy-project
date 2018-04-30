var selectedState = "";
var startDate = 0;
var endDate = 0;
var timePrecision = "";
var parseParameters = {};
var ba = ""

function parseStateColorsAndUpdate() {
    Papa.parse("../data/stateColours.csv", {
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
      $("#radioButtonContainer").load("radioButtons.html");
    });
    console.log("Entered!")

    $( function() {
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
        stateStyles: {fill: '#5F5F5F'},
        stateSpecificStyles: myStyles,

        click: function(event, data) {
            $('#clicked-state').text('You have selected: ' + data.name);
            selectedState = data.name;

            if(oldClass != "") {
                document.getElementById(oldClass).style.display = "none";
            }
            document.getElementById(data.name).style.display = "block";
            oldClass = data.name;
            console.log(data.name);
        },
    });
};

function workOutValues(percent)
{
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
    console.log("Handler for .click called!");
    startDate = $('#startDate').val();
    endDate = $('#endDate').val();
    timePrecision = $(this).val();
    ba = $("input[name='myRadio']:checked").val();
    parseParameters = {
        'startDate' : startDate,
        'endDate' : endDate,
        'state' : selectedState,
        'timePrecision' : timePrecision,
        'BA' : ba
        }
    console.log(parseParameters);
    console.log(Date.parse(startDate));
    console.log(Date.parse(endDate));

    parseData(createGraph, parseParameters);
});

$(document).on('change',"input[name='myRadio']:radio",function(){
    ba = $("input[name='myRadio']:checked").val();
    document.getElementById('dateContainer').style.display = "block";
    document.getElementById('selectionButtonContainer').style.display = "block";
    console.log(ba);
});
