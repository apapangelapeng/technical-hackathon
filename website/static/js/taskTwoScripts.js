function showLoadingStatus() {
    var messages = [
        "Connecting to server...",
        "Gathering data...",
        "Crunching numbers...",
        "Almost there...",
        "Finalizing details..."
    ];
    
    var currentIndex = 0;
    
    function updateStatus() {
        if (currentIndex < messages.length) {
            $('#loading-status').text(messages[currentIndex]);
            currentIndex++;
        } else {
            currentIndex = 0;
        }
    }
    
    // Change the status message every 2 seconds
    var intervalId = setInterval(updateStatus, 2000);
    
    // Function to stop the status updates
    function stopLoadingStatus() {
        clearInterval(intervalId);
        $('#loading-status').text("Request completed.");
    }
    
    return stopLoadingStatus;
}

$(document).ready(function() {

    $('#search-button').click(function() {
        //click search
        performSearch();
    });

    $('#search-box').keypress(function(event) {
        //enter also search
        if (event.which === 13) {
            performSearch();
        }
    });

    function performSearch() {
        let query = $('#search-box').val();
        if (query.trim() === '') {
            alert('Please enter a search term.');
            return;
        }

        // give some user feedback when the button is clicked
        $('#loading-icon').show();
        $('#loading-status').show();
        let stopLoading = showLoadingStatus();

        let tableBody = $('#results-table tbody');
        tableBody.empty();  // Clear previous search results
        if (query.toLowerCase() === 'mock') {
            // Mock response for testing so I don't keep calling and connecting to db and wasting $$
            let mockResponse = {
                results: [
                    { id: 1, name: 'Mock Company A', industry: 'Technology', description: 'Leading technology company Mock' },
                    { id: 2, name: 'Mock Company B', industry: 'Healthcare Mock', description: 'Innovative healthcare mock solutions' }
                ]
            };
            $('#loading-icon').hide();
            stopLoading();
            handleSearchResponse(mockResponse, query);
        } else {
            $.ajax({
                url: '/taskTwoSearch',
                method: 'POST',
                contentType: 'application/json',
                data: JSON.stringify({ query: query }),
                success: function(response) {
                    $('#loading-icon').hide();
                    stopLoading();
                    handleSearchResponse(response, query);
                }
            });
        }
    }

    function handleSearchResponse(response, query) {
        let tableBody = $('#results-table tbody');
        tableBody.empty();
        if (response.results.length === 0) {
            tableBody.append('<tr><td colspan="4">No results found.</td></tr>');
        } else {
            response.results.forEach(profile => {
                let row = `<tr>
                    <td>${profile.id}</td>
                    <td>${profile.name}</td>
                    <td>${profile.industry}</td>
                    <td>${profile.description}</td>
                </tr>`;
                tableBody.append(row);
            });
            highlightMatches(query);
        }
    }

    function highlightMatches(query) {
        $('#results-table tbody tr').each(function() {
            $(this).find('td').each(function() {
                let text = $(this).html();
                let regex = new RegExp(`(${query})`, 'gi');
                let highlightedText = text.replace(regex, '<span class="highlight">$1</span>');
                $(this).html(highlightedText);
            });
        });
    }
});
