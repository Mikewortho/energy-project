/*
 * Parse the data and create a graph with the data.
 */


function parseData(createGraph) {
    Papa.parse("../data/DUK_actual_hourly.csv", {
        download: true,
        complete: function(results) {
            createGraph(results.data);
        }
    });
}

function createGraph(data) {

    var startDate = ["01/07/2015 00:00"];
    var endDate = ["11/04/2016 04:00"];
    //var difference = [];


    var time = [];
    var Demand = ["Predicted Demand "];
    var Forecast = ["U.S Forecast"];
    var Prediction = ["Our Prediction"];
    var b = false;

    for (var i = 1; i < data.length; i++) {

       // _.range(startDate, endDate);
       // time.push(data[i][0]);

        if (data[i][0] == startDate) {           
            b = true; 
            
            }

        if (data[i][0] == endDate){
            time.push(data[i][0]);
            Demand.push(data[i][1]);
            Forecast.push(data[i][2]);
            Prediction.push(data[i][3]);
            
            b = false
            break
            
            }
        
        if (b == true){

        
        time.push(data[i][0]);
        Demand.push(data[i][1]);
        Forecast.push(data[i][2]);
        Prediction.push(data[i][3]);
            }
 
       

    }
   

    console.log(time);
    console.log(Demand);    
    console.log(Forecast);
    console.log(Prediction);
   

    var chart = c3.generate({
            to: '#chart',
            data: {
                columns: 
                
            [   Demand, 
                Forecast, 
                Prediction,
                ]
            },
            axis: {
                x: {
                    type: 'category',
                    categories: time,
                    tick: {
                        multiline: false,
                        culling: {
                            max: 15
                        }
                    }
                }
            },
            zoom: {
                enabled: true
            },
            legend: {
                position: 'right'
            }
      
        });

}

parseData(createGraph);