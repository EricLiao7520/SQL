// A simple JavaScript to create input boxes and run commands typed-in
// by the user.
//
// Copyright (C) 2021 raodm@miamioh.edu

// This is a counter to track the currently active command input from
// the user.
var cmdID = 1;

// This is a global variable used to track the starting time of a query
// to estimate the time taken to get response from the server.
var startTime = 0;

/**
 * This method intercepts and handles the enter key by sending a request
 * to the SQLAir web-serer.
 * 
 * @param {type} event The keystroke event to be handled by this method.
 * @returns {undefined} This method does not return any value.
 */
function enterKey(event) {
    // Check and run command if the enter key is pressed.
    if (event.keyCode === 13) {
        // Cancel the default action, if needed
        event.preventDefault();
        // Disable the input
        event.target.disabled = true;
        // Run the command specified by the user.
        run(event.target.value);
    }
}

function createInput() {
    // Add control to handle to create new div's for each input
    cmdID++;

    // Add new command-input to the HTML.
    var div = document.createElement('div');
    div.id = "" + cmdID;
    div.className = "air_input";
    div.innerHTML = "sql-air> <input id='" + cmdID + 
            "' placeholder='select *;'></div>";
    document.body.appendChild(div);
    // Setup listener to handle the enter key press in this input.
    var input = document.getElementById(cmdID);
    input.addEventListener("keyup", enterKey);
    input.focus();
}

/**
 * This is a helper method that is used to nicely format the response from
 * the AIRServer using HTML tags.  This is just to make the response look
 * nicer on the web-page.
 * 
 * @param {string} resp The raw string response from the server.
 * 
 * @returns {string} An HTML fragment with the response from the server
 * nicely formatted.
 */
function formatResponse(resp) {
    // Record the ending time.
    endTime = new Date().getMilliseconds();
    // Split the multi-line response into an array for convenience
    var lines = resp.split("\n");
    // Remove any empty trailing lines.
    while ((lines.length > 0) && lines[lines.length - 1].length === 0) {
        lines.pop();  // Remove empty blank line at the end.
    }
    // Get the last line which is always a message
    var msg   = lines[lines.length - 1];
    lines.pop();  // Remove the last line.
    
    // Format the message appropriately
    // Single line of response. Either it is an error or a simple
    // message from the server
    var cssClass = (msg.startsWith("Error") ? "error" : "message");
    msg = "<p class='" + cssClass + "'>" + msg + " (Elapsed time: " + 
            (endTime - startTime) + " milliseconds)</p>";
    
    // Format rest of the lines as an HTML table if it is not empty
    var tbl = "";
    if (lines.length > 0) {
        tbl += "<table class='air_table'>\n";
        var start = "<th>";
        var end   = "</th>";
        // Format each line as a table.
        for (var i = 0; (i < lines.length); i++) {
            tbl += "<tr>" + start;
            tbl += lines[i].replace(new RegExp("\t", "g"), end + start);
            tbl += end + "</tr>\n";
            // For subsequent rows use <td></td> 
            start = "<td>";
            end   = "</td>";
        }
        tbl += "</table>\n";
    }
    // Return the formatted response back
    return tbl + msg;
}

/**
 * Helper method to run a given command and also print the response when
 * it is received from the server. It sends the response as an Ajax call.
 * 
 * @param {type} cmd The command to be run on the server.
 * 
 * @returns {undefined} This method does not return any value.
 */
function run(cmd) {
    if (cmd.length > 0) {
        // Run the command using an AJAX request.
        var xhttp = new XMLHttpRequest();
        // Setup handler to add result to the HTML
        xhttp.onreadystatechange = function() {
            if (this.readyState === 4 && this.status === 200) {
                // Nicely format the result.
                var result = formatResponse(this.responseText);
                // Put results in a div.
                var resDiv = document.createElement('div');
                // Add results div to the input div to organize things
                // nicely for the user.
                resDiv.innerHTML = result;
                var output = document.getElementById("" + cmdID);
                output.appendChild(resDiv);
               // Create input box for user to enter the next command to run.
                createInput();
            }
        };
        // Run the command.
        console.log("Running command: " + cmd);
        cmd = encodeURIComponent(cmd);
        xhttp.open("GET", "/sql-air?query=" + cmd, false);
        // Save the tarting time.
        startTime = new Date().getMilliseconds();
        xhttp.send();
    }
}
