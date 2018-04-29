/*
 * Parse the data and create a graph with the data.
 */


function parseData(createGraph) {
    Papa.parse("../data/DUK_hourly_Daily.csv", {
        download: true,
        complete: function(results) {
            createGraph(results.data);
        }
    });
}

function createGraph(data) {

    var startDate = ["01/04/2018 00:00"];
    var endDate = ["14/04/2018 00:00"];
    //var difference = [];


    var time = [];
    var Demand = ["Actual Demand "];
    var Forecast = ["US Forecast"];
    var Prediction = ["Our Prediction"];
    var rf = ["RF Prediction"];
    var mlp = ["MLP Prediction"];
    var svm = ["SVM Prediction"];
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
            rf.push(data[i][4]);
            mlp.push(data[i][5]);
            svm.push(data[i][6]);
            b = false;
            break;

            }

        if (b == true){


        time.push(data[i][0]);
        Demand.push(data[i][1]);
        Forecast.push(data[i][2]);
        Prediction.push(data[i][3]);
        rf.push(data[i][4]);
        mlp.push(data[i][5]);
        svm.push(data[i][6]);
            }



    }


    console.log(time);
    console.log(Demand);
    console.log(Forecast);
    console.log(Prediction);
    console.log(rf);
    console.log(mlp);
    console.log(svm);

    var chart = c3.generate({
            to: '#chart',
            data: {
                columns:

            [   Demand,
                Forecast,
                Prediction,
                rf,
                mlp,
                svm,
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
