/*
 * Parse the data and create a graph with the data.
 */

<<<<<<< HEAD

function parseData(createGraph) {
    Papa.parse("../data/DUK_hourly_Daily.csv", {
=======
function parseData(createGraph, parseParameters) {
    let fileString = "../data/" + parseParameters['BA'] + "_hourly_" + parseParameters['timePrecision'] + ".csv"
    // let fileString = "../data/DUK_hourly_Daily.csv"
    console.log(fileString);
    Papa.parse(fileString, {
>>>>>>> Cameron
        download: true,
        complete: function(results) {
            createGraph(results.data, parseParameters);
        }
    });
}

<<<<<<< HEAD
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

=======
function createGraph(data, parseParameters) {
    let startDate = [parseParameters['startDate'] + " 00:00"];
    let endDate = [parseParameters['endDate'] + " 00:00"];
    console.log(startDate);
    console.log(endDate);
    // let startDate = ["01/04/2018 00:00"];
    // let endDate = ["14/04/2018 00:00"];
    let b = false;
    //let difference = [];


    let time = [];
    let Demand = ["Actual Demand "];
    let Forecast = ["US Forecast"];
    let Prediction = ["Our Prediction"];
    let rf = ["RF Prediction"];
    let mlp = ["MLP Prediction"];
    let svm = ["SVM Prediction"];

    for (let i = 1; i < data.length; i++) {
>>>>>>> Cameron
       // _.range(startDate, endDate);
       // time.push(data[i][0]);

        if (data[i][0] == startDate) {
            b = true;
<<<<<<< HEAD

            }
=======
        }
>>>>>>> Cameron

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

<<<<<<< HEAD

=======
>>>>>>> Cameron
        time.push(data[i][0]);
        Demand.push(data[i][1]);
        Forecast.push(data[i][2]);
        Prediction.push(data[i][3]);
        rf.push(data[i][4]);
        mlp.push(data[i][5]);
        svm.push(data[i][6]);
<<<<<<< HEAD
            }



    }


    console.log(time);
    console.log(Demand);
    console.log(Forecast);
    console.log(Prediction);
    console.log(rf);
    console.log(mlp);
    console.log(svm);
=======
        }
    }

    // console.log(time);
    // console.log(Demand);
    // console.log(Forecast);
    // console.log(Prediction);
    // console.log(rf);
    // console.log(mlp);
    // console.log(svm);
>>>>>>> Cameron

    let chart = c3.generate({
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
<<<<<<< HEAD

parseData(createGraph);
=======
>>>>>>> Cameron
