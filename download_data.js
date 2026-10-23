// Set CSV parser as variable outside of the function. If you put it inside the function, it cannot find variable $.csv
// This uses libraries jquery and jquery-csv, which are included in the html file
var csvparse = $.csv;

// ADD FEATURE DO THIS MORE EFFICIENTLY (read about asychronous calls...)
// https://stackoverflow.com/questions/39545357/unable-to-update-global-variable-inside-a-function-and-call-it-outside-it-jquer
// https://stackoverflow.com/questions/14220321/how-do-i-return-the-response-from-an-asynchronous-call
// Send start and end available dates to the HTML side
// There is a bit of an issue here with the different locations, which all have different start/end dates......
const htmlFilename = window.location.pathname;

if(htmlFilename.includes("weather")){
// use this or that filename. datafile = ;
} else if(htmlFilename.includes("water")){
// use this or that filename. datafile = ;
}

// but for now...
let datafile = "data/test.csv"

    // Now read in data
    $.get(datafile, function(csvdata){
        // Read in data using jquery csv
        let data = csvparse.toArrays(csvdata);

        // Get rows and column length
        const rows = data.length;
        const cols = data[0].length;

        // Original array length
        console.log(data[0].length);

        // View timestamp_utc first
        // console.log(data.map(row => row[0]));

        // Set timestamp_utc as date class
        // Add T and Z to make it an ISO string to make it UTC-ified
        data.slice(1, rows).map(row => row[0] = row[0].replace(" ", "T") + "Z");
        
        // Set timestamp_utc as date class
        data.slice(1, rows).map(row => row[0] = new Date(row[0]));
        
        availablestart = data[1][1].substring(0,10);
        availableend = data[rows-1][1].substring(0,10);

        available_string = "Data available from: " + availablestart + " to " + availableend;

        document.getElementById("availabledates").innerHTML = available_string;
    })

// Function accepts datafile (this is a filepath string)
function download_data(datafile) {

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

    console.log(startDate);
    console.log(endDate);

    // User cannot select future dates
    let today = Date.now();

    if(startDate > today || endDate > today){
        alert("You cannot select future dates!");
    }

    // Now read in data
    $.get(datafile, function(csvdata){
        // Read in data using jquery csv
        let data = csvparse.toArrays(csvdata);

        // Get rows and column length
        const rows = data.length;
        const cols = data[0].length;

        // Original array length
        console.log(data[0].length);

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
            alert("Please make sure the start date comes before the end date!");
        }

        // Select dates where timestamp_utc is greater than or equal to startDate and less than or equal to endDate
        for(let i = 1; i < rows; i++){
            // Extract timestamp value from timestamp_est column (column 1)
            let timestamp = data[i][0];

            // Create column evaluating whether timestamp is between start and end date
            let withinrange = timestamp >= startDate && timestamp <= endDate;
            data[i].push(withinrange);
        }

        // Print last row of data
        console.log(data.map(row => row[13]));
        // Show length of first row
        console.log(data[1].length);

        // Create filtered data where "within range" is true
        let filteredData = data.filter(row => row[13] == true);

        // add headers back on array
        headers = data[0];
        headers.push("withinrange");
        filteredData.unshift(headers);

        // Convert first column back to string for download
        const filteredrows = filteredData.length;
        const filteredcols = filteredData[0].length;

       // Set timestamp utc column back to string
        filteredData.slice(1, filteredrows).map(row => row[0] = row[0].toISOString().replace('T', ' ').substring(0, 19));

        console.log(filteredData.map(row => row[0]));

        // Drop last column
        filteredData = filteredData.map(row => row.slice(0, -1));

        // Convert array back to CSV using jquery csv
        const outputcsv = csvparse.fromArrays(filteredData);
        // console.log(outputcsv);

        // create filename for download
        const firstdate = filteredData[1][1].replaceAll(":", "").replaceAll(" ", "_");
        const lastdate = filteredData[filteredrows-1][1].replaceAll(":", "").replaceAll(" ", "_");
        const filename = "waterquality_data_" + firstdate + "_" + lastdate + ".csv";

        // console.log(filename);

        // trigger download of CSV file 
        const blob = new Blob([outputcsv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();

        // ADD FEATURE Write alert for if data does not exist for that time frame

        // ADD FEATURE if file contains XXX add water quality to filename, otherwise add weather to filename
    })
}




// I wrote this code for NodeJS polars but it only works server-side, which I think will ultimately be unnecessary...


// import pl, { DataFrame } from 'nodejs-polars';

// let df = pl.readCSV("data/test.csv",{
//     hasHeader: true,
//     sep: ",",
//     parseDates: true,
//     inferSchemaLength: 0
// });

// let startDate = "2025-01-01 00:00:00";
// let endDate = "2026-01-05 00:00:00";

// startDate = new Date(startDate);
// endDate = new Date(endDate);

// df = df.withColumn(pl.col("timestamp_utc").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"));

// const test = df.filter(
//     pl.col("timestamp_utc").gtEq(pl.lit(startDate)).and(
//         pl.col("timestamp_utc").ltEq(endDate)));

// console.log(test.head(5));
// console.log(test.tail(5));
// console.log(startDate);
// console.log(endDate);

// const button = document.getElementById('download-btn');
// button.addEventListener('click', download);

// finalstring = startDate + "_" + endDate;
// document.getElementById("JSmessage").innerHTML = dat_head

