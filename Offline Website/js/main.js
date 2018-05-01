<<<<<<< HEAD

function parseData2() {
=======
var selectedState = "";
var startDate = 0;
var endDate = 0;
var timePrecision = "";
var parseParameters = {};
var ba = ""

function parseStateColorsAndUpdate() {
>>>>>>> Cameron
    Papa.parse("../data/stateColours.csv", {
        download: true,
        complete: function(results) {
            var resultsData = results.data
            printMap(resultsData)
        }
    })
}
<<<<<<< HEAD
$(document).ready(function() {
    parseData2()
    console.log("Entered!")
=======

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
>>>>>>> Cameron
});

function printMap(data)
{
    // Array to object.
    var oldClass = ""
    var myStyles = {}
<<<<<<< HEAD
    for (var i = 1; i < data.length; i++) {
        myStyles[data[i][0]] = {fill: workOutValues(data[i][1])};
    }
=======

    for (var i = 1; i < data.length; i++) {
        myStyles[data[i][0]] = {fill: workOutValues(data[i][1])};
    }

    $('#map').usmap({
        stateStyles: {fill: '#5F5F5F'},
        stateSpecificStyles: myStyles,
>>>>>>> Cameron

    $('#map').usmap({
        stateStyles: {fill: '#5F5F5F'},
        stateSpecificStyles: myStyles,
        click: function(event, data) {
<<<<<<< HEAD
            $('#clicked-state')
            .text('You clicked: ' + data.name);
            if(oldClass != "")
                document.getElementById(oldClass).style.display = "none";
            document.getElementById(data.name).style.display = "block";
            oldClass = data.name;
            console.log(data.name);
            updateChart();

=======
            $('#clicked-state').text('You have selected: ' + data.name);
            selectedState = data.name;

            if(oldClass != "") {
                document.getElementById(oldClass).style.display = "none";
            }
            document.getElementById(data.name).style.display = "block";
            oldClass = data.name;
            console.log(data.name);
>>>>>>> Cameron
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
<<<<<<< HEAD
    });
}

function updateChart() {
    var $image = $("img").first();
    if ($image.attr("src") == "../site/img/chart01.jpg") {
        $image.attr("src", "../site/img/chart02.jpg");
    }
    else (
        $image.attr("src", "../site/img/chart02.jpg")
    )
}

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

// $('.radioButton').click(function(this) {
//     ($this).val()
// });
//
// $('.radioButton').click(setShape);
//
// function setShape() {
//     var BA  = $('.radionButton:checked').val();
//     }
//
// {
//     ($this).val()
// });

$('.radioButton').click(function() {
    var BA  = $('.radioButton:checked').val();
    console.log(BA + 'HI')
=======
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
>>>>>>> Cameron
});
