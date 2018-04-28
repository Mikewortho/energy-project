
function parseData2() {
    Papa.parse("../data/stateColours.csv", {
        download: true,
        complete: function(results) {
            var resultsData = results.data
            printMap(resultsData)
        }
    })
}
$(document).ready(function() {
    parseData2()
    console.log("Entered!")
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
            $('#clicked-state')
            .text('You clicked: ' + data.name);
            if(oldClass != "")
                document.getElementById(oldClass).style.display = "none";
            document.getElementById(data.name).style.display = "block";
            oldClass = data.name;
            console.log(data.name);
            updateChart();

        },

        // the hover action
        mouseover: function(event, data) {
            $('#clicked-state')
            .text('You hovered: '+data.name);
            console.log(data.name);
        }
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
});
