// Set CSV parser as variable outside of the function. If you put it inside the function, it cannot find variable $.csv
// This uses libraries jquery and jquery-csv, which are included in the html file
var csvparse = $.csv;

// Function accepts datafile (this is a filepath string)
function create_graph(datafile) {

    // Get start and end date from HTML input
    let startDate = document.getElementById("startdate").value;
    let endDate = document.getElementById("enddate").value;
    
    // Change end Date to end of that day rather than start (00:00:00)
    // Set to date class in JS
    // For some reason, adding timestamp to the date keeps it in EST internally
    if(startDate != ""){
        startDate = startDate + " 00:00:00";
        startDate = new Date(startDate);
    }

    if(endDate != ""){
        endDate = endDate + " 23:59:59";
        endDate = new Date(endDate);
    }

    // User cannot select future dates
    let today = new Date();
    today = today.toISOString().split('T')[0] +  " 23:59:59";
    today = new Date(today);

    if(startDate > today || endDate > today){
        alert("You cannot select future dates.");
        throw new Error("future date selected");
    }


    // Now read in data
    $.get(datafile, function(csvdata){
        // Read in data using jquery csv
        let data = csvparse.toArrays(csvdata);

        // Get rows and column length
        const rows = data.length;
        const cols = data[0].length;

        // View timestamp_utc first
        // console.log(data.map(row => row[0]));

        // Set timestamp_utc as date class
        // Add T and Z to make it an ISO string to make it UTC-ified
        data.slice(1, rows).map(row => row[0] = row[0].replace(" ", "T") + "Z");
        
        // Set timestamp_utc as date class
        data.slice(1, rows).map(row => row[0] = new Date(row[0]));

        // console.log(data.map(row => row[0]));

        // If user does not select start or end time, set start and end to earliest and latest available times
        if(startDate == ""){
            startDate = data[1][0];
            console.log(startDate);
        } 
        
        if(endDate == ""){
            endDate = data[rows-1][0];
            console.log(endDate);
        }

        // Address case where user selects end date that comes before start date
        if(startDate > endDate) {
            alert("Please make sure the start date comes before the end date.");
            throw new Error("invalid date range");
        }

        // Create a plotly from the filtered data
        // Get the column name that was selected
        let selected_param = document.getElementById("WeatherSelect").value;
        let col_match = data[0].indexOf(selected_param);

        // Get unique sites
        let site_col = data[0].indexOf("station");
        let sites = [...new Set(data.slice(1,rows).map(row => row[site_col]))];

        // Create empty array to hold traces
        const trace_array = new Array(sites.length);

        for(let i = 0; i < sites.length; i++){
            let site = sites[i];
            let site_data = data.filter(row => row[site_col] == site);

            trace_array[i] = {
                x: site_data.map(row => row[0]),
                y: site_data.map(row => row[col_match]),
                mode: "lines",
                name: site,
                connectgaps: false
            }
        }




        // var trace1 = {
        //     x: data.map(row => row[0]),
        //     y: data.map(row => row[col_match]),
        //     mode: "lines"
        // };

        // var plot_data = [trace1];

        var layout = {
                margin: {t: 30},
                xaxis: {range: [startDate, endDate]}
        };

        Plotly.newPlot(document.getElementById('testplot'), trace_array, layout);

        // console.log(col_match);
        // console.log(selected_param);
    })
}