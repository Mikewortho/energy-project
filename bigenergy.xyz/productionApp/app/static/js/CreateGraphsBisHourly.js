/*
 * Parse the data and create a graph with the data.
 */

function parseData(createGraph, parseParameters) {
    let fileString = parseParameters['BA'] + "_hourly_" + parseParameters['timePrecision'] + ".csv"
    // let fileString = "../data/DUK_hourly_Daily.csv"
    console.log(fileString);
    $.get("http://playingthefield.xyz/bigenergy/data/" + fileString)
    .done(function(returnData) {
        var graphData = Papa.parse(returnData);
        createGraph(graphData.data, parseParameters);
    })
    // Papa.parse(fileString, {
    //     download: true,
    //     complete: function(results) {
    //         createGraph(results.data, parseParameters);
    //     }
    // });
}

function parseStats(parseParameters) {
    let fileString = parseParameters['BA'] + "_settings_" + parseParameters['timePrecision'] + ".txt"
    // let fileString = "../data/DUK_hourly_Daily.csv"
    console.log(fileString);
    $.get("http://playingthefield.xyz/bigenergy/data/" + fileString)
    .done(function(returnData) {
        var statsData = Papa.parse(returnData);
        printStats(statsData.data);
    })
    // Papa.parse(fileString, {
    //     download: true,
    //     complete: function(results) {
    //         printStats(results.data);
    //     }
    // });
}

function printStats(data)
{
    let firstRow = data[0]
    let maxVal = data[1][0]
    let maxIndex = 1
    let maxRow = data[1]
    for (let i = 2; i < data.length; i++)
    {
        if(data[i][0]>maxVal)
        {
            maxVal = data[i][0]
            maxRow = data[i]
            maxIndex = i
        }
    }
    typeOfPrediction = ["US Prediction","Arima Regression","RF Regression","RF Regression","SVM Regression","MLP Regression","MLP Regression"]

    for(let j = 0; j < maxRow.length; j++)
    {
        if(maxRow[j] > 0)
            maxRow[j] = "+"+(Math.round(maxRow[j] * 100)/100)
        else
            maxRow[j] = (Math.round(maxRow[j] * 100)/100)
    }
    maxRow = [typeOfPrediction[maxIndex]].concat(maxRow)
    for(let j = 0; j < firstRow.length; j++)
    {
        if(firstRow[j] > 0)
            firstRow[j] = "+"+(Math.round(firstRow[j] * 100)/100)
        else
            firstRow[j] = (Math.round(firstRow[j] * 100)/100)
    }
    firstRow = [typeOfPrediction[0]].concat(firstRow)
    console.log(firstRow)
    console.log(maxRow)
    updateAndDisplayStatsSidebar(firstRow, maxRow)
}

function updateAndDisplayStatsSidebar(firstRow, maxRow) {
    $('#statsTable').append('<tr><td>' + firstRow[0] + '</td>' +
                                 '<td class="rightAligned">' + firstRow[1] + '</td>' +
                                 '<td class="rightAligned">' + firstRow[2] + '</td>' +
                                 '<td class="rightAligned">' + firstRow[3] + '</td></tr>')

     $('#statsTable').append('<tr><td>' + maxRow[0] + '</td>' +
                                  '<td class="rightAligned">' + maxRow[1] + '</td>' +
                                  '<td class="rightAligned">' + maxRow[2] + '</td>' +
                                  '<td class="rightAligned">' + maxRow[3] + '</td></tr>')
    document.getElementById('statsContainer').style.display = "block"; //
}

function createGraph(data, parseParameters)
{
    console.log(data);
    let startDateArray = parseParameters['startDate'].split('/')
    let endDateArray = parseParameters['endDate'].split('/')

    let startDate = startDateArray[2] + "-" + startDateArray[1] + "-" + startDateArray[0] + " 00:00:00"
    let endDate = endDateArray[2] + "-" + endDateArray[1] + "-" + endDateArray[0] + " 00:00:00"

    earliestPos = data[1][0].split(/-| /).slice(0,3)
    latestPos = data[data.length-2][0].split(/-| /).slice(0,3)
    startDateStart = startDate.split(/-| /).slice(0,3)
    startDateEnd = endDate.split(/-| /).slice(0,3)
    error = 0
    outputStatement = ""
    if(startDateEnd[0]<startDateStart[0])
    {
        // are they inverted?
        outputStatement += "End date is before start date\n"
        error = 1
    }
    else if (startDateEnd[0]==startDateStart[0]&&startDateEnd[1]<startDateStart[1])
    {
        // are they inverted?
        outputStatement += "End date is before start date\n"
        error = 1
    }
    else if (startDateEnd[0]==startDateStart[0]&&startDateEnd[1]==startDateStart[1]&&startDateEnd[2]<startDateStart[2])
    {
        // are they inverted?
        outputStatement += "End date is before start date\n"
        error = 1
    }
    if(startDateEnd[0]<earliestPos[0])
    {
        // is start specified less than the earliest possible
        outputStatement += "End date is before earliest avaiable start date "+data[1][0]+"\n"
        error = 1
    }
    else if (startDateEnd[0]==earliestPos[0]&&startDateEnd[1]<earliestPos[1])
    {
        // are they inverted?
        outputStatement += "End date is before earliest avaiable start date "+data[1][0]+"\n"
        error = 1
    }
    else if (startDateEnd[0]==earliestPos[0]&&startDateEnd[1]==earliestPos[1]&&startDateEnd[2]<earliestPos[2])
    {
        // are they inverted?
        outputStatement += "End date is before earliest avaiable start date "+data[1][0]+"\n"
        error = 1
    }
    if(startDateStart[0]>latestPos[0])
    {
        // is the start date specified later than the latest possible
        outputStatement += "Start date is after latest avaiable date"+data[data.length-2][0]+"\n"
        error = 1
    }
    else if(startDateStart[0]==latestPos[0]&&startDateStart[1]>latestPos[1])
    {
        // is the start date specified later than the latest possible
        outputStatement += "Start date is after latest avaiable date"+data[data.length-2][0]+"\n"
        error = 1
    }
    else if(startDateStart[0]==latestPos[0]&&startDateStart[1]==latestPos[1]&&startDateStart[1]>latestPos[1])
    {
        // is the start date specified later than the latest possible
        outputStatement += "Start date is after latest avaiable date"+data[data.length-2][0]+"\n"
        error = 1
    }
    if(error == 1)
    {
        document.getElementById("chart").style.display = "none";
        alert(outputStatement)
        return
    }
    if(startDateStart[0]<earliestPos[0])
    {
        startDate = data[1][0]
    }
    else if(startDateStart[0]==earliestPos[0]&&startDateStart[1]<earliestPos[1])
    {
        startDate = data[1][0]
    }
    else if(startDateStart[0]==earliestPos[0]&&startDateStart[1]==earliestPos[1]&&startDateStart[2]<earliestPos[2])
    {
        startDate = data[1][0]
    }

    var time = ["x"];
    var Demand = ["Actual Demand "];
    var Forecast = ["U.S Forecast"];
    var Prediction = ["Arima Prediction"];
    var RF_Prediction = ["RF Prediction"];
    var MLP_Prediction = ["MLP Prediction"];
    var SVM_Prediction = ["SVM Prediction"];

    var b = false;
    for (var i = 1; i < data.length; i++) {
        if(data[i][0]=="") {
            b = false
            break
        }

        if (data[i][0] == startDate) {
            b = true;
        }

        if (data[i][0] == endDate) {
            time.push(data[i][0]);
            Demand.push(data[i][1]);
            Forecast.push(data[i][2]);
            Prediction.push(data[i][3]);
            RF_Prediction.push(data[i][4]);
            MLP_Prediction.push(data[i][5]);
            SVM_Prediction.push(data[i][6]);

            b = false
            break
        }

        if (b == true) {
            time.push(data[i][0]);
            Demand.push(data[i][1]);
            Forecast.push(data[i][2]);
            Prediction.push(data[i][3]);
            RF_Prediction.push(data[i][4]);
            MLP_Prediction.push(data[i][5]);
            SVM_Prediction.push(data[i][6]);
        }
    }

    var chart = c3.generate({
        to: '#chart',
        data: {
            x: 'x',
            xFormat: '%Y-%m-%d %H:%M:%S', // 'xFormat' can be used as custom format of 'x'
            columns:

        [
            time,
            Demand,
            Forecast,
            Prediction,
            RF_Prediction,
            MLP_Prediction,
            SVM_Prediction,
            ]
        },
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    format: '%Y-%m-%d %H:%M:%S',
                    count: 10,
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
    document.getElementById("chart").style.display = "block";
    window.scrollTo(0,document.body.scrollHeight);
}
