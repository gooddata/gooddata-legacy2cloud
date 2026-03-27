// (C) 2026 GoodData Corporation
// Apply combined filter and search to table rows
function applyTableFilter(filter) {
    const table = document.getElementById('migrationTable');
    const rows = table.querySelectorAll('tbody tr');

    rows.forEach(row => {
        // Skip description rows
        if (row.classList.contains('description-row')) {
            return;
        }

        let matchesFilter = filter === 'all' || row.classList.contains('status-' + filter);
        let matchesSearch = true;

        // Apply search filter if there's a search term
        if (currentSearchTerm) {
            matchesSearch = false;
            const cells = row.getElementsByTagName('td');

            for (let i = 0; i < cells.length; i++) {
                if (cells[i].textContent.toLowerCase().indexOf(currentSearchTerm) > -1) {
                    matchesSearch = true;
                    break;
                }
            }
        }

        // Show row only if it matches both filter and search
        if (matchesFilter && matchesSearch) {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }

        // Also hide corresponding description row if main row is hidden
        const descRow = document.getElementById(row.id + '-desc');
        if (descRow) {
            descRow.style.display = (row.style.display === '' && row.classList.contains('expanded')) ? 'table-row' : 'none';
        }
    });

    // If comparison view is open, update navigation buttons to account for filtered rows
    if (isComparisonViewOpen) {
        const currentRow = document.getElementById(currentComparisonRowId);
        const comparableRows = getComparableRows();

        // If no comparable rows remain, disable both buttons
        if (comparableRows.length === 0) {
            const prevButton = document.getElementById('prevCompareButton');
            const nextButton = document.getElementById('nextCompareButton');
            prevButton.disabled = true;
            nextButton.disabled = true;
        }
        // If current row is still visible, update navigation relative to it
        else if (currentRow && currentRow.style.display !== 'none') {
            setupNavigationButtons(currentComparisonRowId);
        }
        // If current row is filtered out, reset to first visible item
        else {
            // Get first visible comparable row
            const firstVisibleRow = comparableRows[0];
            if (firstVisibleRow) {
                // Highlight this row but don't trigger comparison (which would reload iframes)
                highlightRow(firstVisibleRow.id);
                currentComparisonRowId = firstVisibleRow.id;
                // Update navigation buttons
                setupNavigationButtons(firstVisibleRow.id);
            }
        }
    }
}

// Get all rows with compare buttons that are currently visible
function getComparableRows() {
    const allRows = document.querySelectorAll('#migrationTable tbody tr');
    const comparableRows = Array.from(allRows).filter(row => {
        const compareBtn = row.querySelector('.compare-btn');
        // Make sure the row is visible (not filtered out)
        return compareBtn !== null && row.style.display !== 'none';
    });
    return comparableRows;
}

// Table sorting functionality
document.addEventListener('DOMContentLoaded', function() {
    const table = document.getElementById('migrationTable');
    const tableBody = table.querySelector('tbody');
    const headers = table.querySelectorAll('thead th');
    const rows = Array.from(table.querySelectorAll('tbody tr')).filter(row => !row.classList.contains('description-row'));
    const directions = {};

    // Function to sort the table
    function sortTable(columnIndex, direction) {
        const multiplier = (direction === 'asc') ? 1 : -1;

        // Sort rows based on cell content
        const sortedRows = rows.sort(function(rowA, rowB) {
            // Special handling for Status column (index 1)
            if (columnIndex === 1) {
                // Sort by status priority: API_ERROR > error > warning > skipped > inherited > success
                const getStatusPriority = function(row) {
                    if (row.classList.contains('status-api-error')) return 0;
                    if (row.classList.contains('status-error')) return 1;
                    if (row.classList.contains('status-warning')) return 2;
                    if (row.classList.contains('status-skipped')) return 3;
                    if (row.classList.contains('status-inherited')) return 4;
                    return 5; // success
                };

                const priorityA = getStatusPriority(rowA);
                const priorityB = getStatusPriority(rowB);

                return (priorityA - priorityB) * multiplier;
            }

            // Special handling for ordinal number column (index 0)
            if (columnIndex === 0) {
                const textA = rowA.querySelectorAll('td')[columnIndex].textContent.trim();
                const textB = rowB.querySelectorAll('td')[columnIndex].textContent.trim();

                // Check if both values are pure numbers (no prefix)
                const isNumericA = /^\d+$/.test(textA);
                const isNumericB = /^\d+$/.test(textB);

                // If both are numeric, compare as numbers
                if (isNumericA && isNumericB) {
                    return (parseInt(textA) - parseInt(textB)) * multiplier;
                }

                // Extract numeric parts from prefixed values
                const numericPartA = textA.match(/^[^\d]*(\d+)$/);
                const numericPartB = textB.match(/^[^\d]*(\d+)$/);

                // If only one is numeric (no prefix), numeric values should come first
                if (isNumericA && !isNumericB) {
                    return -1 * multiplier; // A comes before B when ascending
                }
                if (!isNumericA && isNumericB) {
                    return 1 * multiplier; // B comes before A when ascending
                }

                // If both have prefixes and we can extract numeric parts, compare those parts
                if (numericPartA && numericPartB) {
                    return (parseInt(numericPartA[1]) - parseInt(numericPartB[1])) * multiplier;
                }

                // Fallback to string comparison
                return textA.localeCompare(textB) * multiplier;
            }

            // Get text content from the column
            const cellA = rowA.querySelectorAll('td')[columnIndex].textContent.trim();
            const cellB = rowB.querySelectorAll('td')[columnIndex].textContent.trim();

            // Regular alpha sorting for other columns
            return cellA.localeCompare(cellB) * multiplier;
        });

        // Remove all rows from table body
        while (tableBody.firstChild) {
            tableBody.removeChild(tableBody.firstChild);
        }

        // Append sorted rows
        sortedRows.forEach(function(row) {
            tableBody.appendChild(row);
        });

        // Update navigation buttons if comparison is open
        if (isComparisonViewOpen && currentComparisonRowId) {
            // If current row still exists, update navigation
            if (document.getElementById(currentComparisonRowId)) {
                setupNavigationButtons(currentComparisonRowId);
                highlightRow(currentComparisonRowId);
            } else {
                // If row no longer exists (filtered out), close comparison
                closeComparison();
            }
        }
    }

    // Add click event for all headers
    headers.forEach(function(header, index) {
        // Set first column as initially sorted (show ascending arrow)
        if (index === 0) {
            header.classList.add('sort-asc');
        }

        header.addEventListener('click', function() {
            // Reset all headers
            headers.forEach(h => {
                h.classList.remove('sort-asc', 'sort-desc');
            });

            // Get the sort direction
            const direction = directions[index] || 'asc';
            const multiplier = (direction === 'asc') ? 1 : -1;

            // Update header class for styling
            this.classList.toggle('sort-' + direction);

            // Update direction for next click
            directions[index] = direction === 'asc' ? 'desc' : 'asc';

            // Sort the table
            sortTable(index, direction);
        });
    });

    // Initially sort the table by the first column ascending
    sortTable(0, 'asc');
});
