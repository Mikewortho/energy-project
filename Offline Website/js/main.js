$(document).ready(function() {
    $('#map').usmap({
        stateStyles: {fill: '#95ccff'},
        stateHoverStyles: {fill: '#e0f4f2'},
        stateHoverAnimation: 100,

        click: function(event, data) {
            $('#clicked-state')
            .text('You clicked: ' + data.name);
            console.log(data.name);
            updateChart();
            $('#map').usmap({stateStyles: {fill: 'red'}});
        },

        // the hover action
        mouseover: function(event, data) {
            $('#clicked-state')
            .text('You hovered: '+data.name);
            console.log(data.name);
        }
    });

    console.log("Entered!")
});

function updateChart() {
    var $image = $("img").first();
    if ($image.attr("src") == "../site/img/chart01.jpg") {
        $image.attr("src", "../site/img/chart02.jpg");
    }
    else (
        $image.attr("src", "../site/img/chart02.jpg")
    )
}
